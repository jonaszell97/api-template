from __future__ import annotations

from abc import ABC
from datetime import datetime, timezone
from enum import Enum
import os
from typing import Awaitable, Callable, Optional, Tuple, TypeVar, Type, Generic, Any

from sqlalchemy import Delete, Select, delete, event, func, or_
from sqlalchemy.orm import InstrumentedAttribute
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from starlette import status
from fastapi import APIRouter, Body, Depends, HTTPException, Request
from fastapi.encoders import jsonable_encoder
from fastapi_pagination import Page
from fastapi_pagination.ext.sqlalchemy import paginate

from app import schemas, types
from app.database import Base as ModelBase, get_db_session

from app.lib.utility.authentication import AuthorizedUser, requires_auth
from app.lib.utility.query_utils import FiltersType, apply_filters, apply_sort, SortOrder
from app.lib.utility.validation_errors import CustomValidationError
from app.lib.utility.websockets import websocket_manager
from app.lib.utility.logging import logger


_ModelVar = TypeVar("_ModelVar", bound=ModelBase)
_ReadSchemaVar = TypeVar("_ReadSchemaVar", bound=schemas.BaseModel)
_UpdateSchemaVar = TypeVar("_UpdateSchemaVar", bound=schemas.BaseModel)
_CreateSchemaVar = TypeVar("_CreateSchemaVar", bound=schemas.BaseModel)
_ValidateCreateSchemaVar = TypeVar("_ValidateCreateSchemaVar", bound=schemas.BaseModel)
_ValidateUpdateSchemaVar = TypeVar("_ValidateUpdateSchemaVar", bound=schemas.BaseModel)


class CrudOperation(Enum):
    CREATE = "create"
    READ = "read"
    UPDATE = "update"
    DELETE = "delete"

# the TS rank normalisation takes the length of a string into account when ranking
# concretely it divides the rank by 1 + the logarithm of the string length (1 / (1 + log(length)))
# see https://www-postgresql-org.translate.goog/docs/current/textsearch-controls.html?_x_tr_sl=en&_x_tr_tl=de&_x_tr_hl=de&_x_tr_pto=sc#TEXTSEARCH-RANKING for more information
TS_RANK_NORMALIZATION = 1
TS_VECTOR_LANGUAGE = os.getenv("TS_VECTOR_LANGUAGE", "english")

class CrudController(
    Generic[
        _ModelVar,
        _ReadSchemaVar,
        _UpdateSchemaVar,
        _CreateSchemaVar,
        _ValidateUpdateSchemaVar,
        _ValidateCreateSchemaVar,
        
    ], ABC):
    """
    Base class for CRUD operations on a database model.
    """

    # List of all CRUD classes
    all_cruds: list['CrudController'] = []
    
    # data migration functions
    _migration_functions: list[Callable[[AsyncSession], Awaitable[None]]] = []

    def __init__(
        self,
        route: Optional[str],
        model_type: Type[_ModelVar],
        read_type: Type[_ReadSchemaVar],
        update_type: Type[_UpdateSchemaVar],
        create_type: Type[_CreateSchemaVar],
        validate_update_type: Optional[Type[_ValidateUpdateSchemaVar]] = None,
        validate_create_type: Optional[Type[_ValidateCreateSchemaVar]] = None,
        user_filter_property: Optional[str] = None,
        websocket_update_user_property: Optional[str] = None,
        websocket_update_user_roles: Optional[list[types.UserRoles]] = None,
        websocket_updates: bool = True,
        supports_search: bool = False,
        substring_search_fields: Optional[list[InstrumentedAttribute[Any]]] = None,
        readable_identifier_column: Optional[InstrumentedAttribute[str]] = None,
        is_core_model: bool = False,
        relationships: Optional[list[schemas.ModelRelationship]] = None,
        parent_crud: Optional[CrudController] = None,
    ):
        """
        Initialize a new CRUD class.
        - `model_type`: The SQLAlchemy model class to operate on.
        - `read_type`: The Pydantic schema to use for reading data from the database.
        - `update_type`: The Pydantic schema to use for updating data in the database.
        - `create_type`: The Pydantic schema to use for creating new data in the database.
        - `validate_type`: The Pydantic schema to use for validating data.
        - `user_filter_property`: If given, read queries will only return rows where
            this property matches the current user's email.
        - `websocket_update_user_property`: If given, the CRUD class will send websocket updates
            only to users whose email matches the property value
            of the updated row.
        - `websocket_update_user_roles`: If given, the CRUD class will send websocket updates
            only to users with the given roles.
        - `websocket_updates`: If False, no websocket updates will be sent for this CRUD class.
        - `supports_search`: If True, the CRUD class supports full-text search.
        - `substring_search_fields`: A list of fields to search for substrings.
        - `readable_identifier_column`: The name of a column that can uniquely identify a row in a way that is
            understandable to the user (e.g. a unique name). Not all CRUD classes have such a column.
        - `is_core_model`: Whether this is a core data model. Core data models have a tracked change history,
            and require approval for modifications.
        """
        self.route = route
        self.model_type = model_type
        self.read_type = read_type
        self.update_type = update_type
        self.create_type = create_type
        self.websocket_update_user_property = websocket_update_user_property
        self.websocket_update_user_roles = websocket_update_user_roles
        self.user_filter_property = user_filter_property
        self.supports_search = supports_search
        self.substring_search_fields = substring_search_fields or []
        self.readable_identifier_column = readable_identifier_column
        self.is_core_model = is_core_model
        self.inherits_from = parent_crud.table_name if parent_crud else None
        
        # relationships
        self.relationships = [
            *(CrudController._get_inherited_relationships(parent_crud, model_name=model_type.__tablename__) if parent_crud else []),
            *(relationships if relationships else []),
        ]
        
        CrudController._check_relationship_definitions(model_name=model_type.__tablename__, relationships=self.relationships)
        
        if validate_update_type is not None:
            self.validate_update_type: Type[_ValidateUpdateSchemaVar] = validate_update_type
        else:
            any_update_type: Any = update_type
            self.validate_update_type: Type[_ValidateUpdateSchemaVar] = any_update_type
            
        if validate_create_type is not None:
            self.validate_create_type: Type[_ValidateCreateSchemaVar] = validate_create_type
        else:
            any_create_type: Any = create_type
            self.validate_create_type: Type[_ValidateCreateSchemaVar] = any_create_type

        if websocket_updates:
            @event.listens_for(model_type, "after_update")
            @event.listens_for(model_type, "after_insert")
            def update_listener(mapper, connection, target: _ModelVar):
                task_schema = read_type.model_validate(target)
                json_data = jsonable_encoder(task_schema)
                id = getattr(target, "id")

                websocket_manager.register_update(
                    model_type.__tablename__,
                    json_data,
                    id,
                    deleted=False,
                )

            @event.listens_for(model_type, "after_delete")
            def delete_listener(mapper, connection, target: _ModelVar):
                task_schema = read_type.model_validate(target)
                json_data = jsonable_encoder(task_schema)
                id = getattr(target, "id")

                websocket_manager.register_update(
                    model_type.__tablename__,
                    json_data,
                    id,
                    deleted=True,
                )

            self.update_listener = update_listener
            self.delete_listener = delete_listener
        else:
            self.update_listener = None
            self.delete_listener = None

        CrudController.all_cruds.append(self)


    @staticmethod
    def unregister_events():
        logger.info("Unregistering events")
        for crud in CrudController.all_cruds:
            crud.unregister_event_listeners()


    def unregister_event_listeners(self):
        """
        Unregister the event listeners for this CRUD class.
        """
        if self.update_listener:
            event.remove(self.model_type, "after_update", self.update_listener)
            event.remove(self.model_type, "after_insert", self.update_listener)

        if self.delete_listener:
            event.remove(self.model_type, "after_delete", self.delete_listener)

    @property
    def table_name(self):
        return self.model_type.__tablename__

    @staticmethod
    def register_migration_function(func: Callable[[AsyncSession], Awaitable[None]]):
        CrudController._migration_functions.append(func)
        
    
    @staticmethod
    async def execute_migrations(db: AsyncSession):
        for func in CrudController._migration_functions:
            logger.info(f"Executing migration function {func.__name__}")
            await func(db)

    @staticmethod
    def get_crud_controller(table_name) -> Optional['CrudController']:
        for crud in CrudController.all_cruds:
            if crud.model_type.__tablename__ == table_name:
                return crud
            
        return None

    def _apply_filters(
        self,
        operation: CrudOperation,
        query,
        filters: FiltersType,
        current_user: Optional[AuthorizedUser] = None,
    ):
        """
        Apply filters to a query.
        """
        if "bypass_user_filter" in filters:
            if filters["bypass_user_filter"]:
                del filters["bypass_user_filter"]
                return apply_filters(filters, query, self.model_type)
            else:
                del filters["bypass_user_filter"]
        
        if self.user_filter_property:
            if current_user and not current_user.is_admin:
                filters[self.user_filter_property] = current_user.email

        return apply_filters(filters, query, self.model_type)

    # ---------- Create ----------

    async def _get_database_values_from_create_model(
        self,
        db: AsyncSession,
        to_create: _CreateSchemaVar,
    ) -> dict[str, Any]:
        """
        Get the values from the database that are needed to create a new object.
        """
        return to_create.model_dump()

    async def create(
        self,
        db: AsyncSession,
        to_create: _CreateSchemaVar,
        current_user: Optional[AuthorizedUser] = None,
        approved: bool = False,
    ) -> _ModelVar:
        """
        Create a new object in the database
        """
        assert isinstance(to_create, schemas.BaseModel)

        create_values = await self._get_database_values_from_create_model(db, to_create)
        db_value = self.model_type(**create_values)
        
        # check for approval on core data models
        if self.is_core_model and not approved:
            setattr(db_value, "approved", False)
        
        # save the value
        db.add(db_value)
        await db.flush()
        await db.refresh(db_value)

        result = await self.read_by_id(db, getattr(db_value, "id"))
        assert result is not None, "Value not found"
        
        return result

    # ---------- Read ----------

    def apply_select_options(self, query: Select[Tuple[_ModelVar]]) -> Select[Tuple[_ModelVar]]:
        """
        Apply additional options to the select query, e.g. to select in additional columns.
        """
        return query
    
    def apply_default_ordering(self, query: Select[Tuple[_ModelVar]]) -> Select[Tuple[_ModelVar]]:
        """
        Apply default ordering to the query.
        """
        return query

    def generate_value_summary(self, value: _ModelVar) -> str:
        if self.readable_identifier_column is not None:
            return getattr(value, self.readable_identifier_column.key)
        
        return f"{value.id}"
    
    
    async def read(
        self,
        db: AsyncSession,
        filters: Optional[FiltersType] = None,
        sort_field: Optional[str] = None,
        sort_order: Optional[SortOrder] = None,
        limit: Optional[int] = None,
        current_user: Optional[AuthorizedUser] = None,
        approved_only: bool = False,
    ) -> list[_ModelVar]:
        """
        Read all rows from the database that match the given filters. Optionally sort the results.
        If current_user and user_filter_property are set, only return rows where the user_filter_property
        matches the current_user's email.
        """
        filters = filters or {}
        
        query = select(self.model_type)
        query = self.apply_select_options(query)

        # filtering
        if approved_only and self.is_core_model:
            filters["approved"] = True
            
        query = self._apply_filters(CrudOperation.READ, query, filters, current_user)

        # sorting
        if sort_field is not None and sort_order is not None:
            query = apply_sort(sort_field, sort_order, query, self.model_type)
        else:
            query = self.apply_default_ordering(query) # type: ignore

        # limit
        if limit is not None:
            query = query.limit(limit)

        return list((await db.execute(query)).scalars().all())


    async def find(
        self,
        db: AsyncSession,
        filters: Optional[FiltersType] = None,
        sort_field: Optional[str] = None,
        sort_order: Optional[SortOrder] = None,
        current_user: Optional[AuthorizedUser] = None,
        approved_only: bool = False,
    ) -> Optional[_ModelVar]:
        """
        Find the first row that matches the given filters. Optionally sort the results.
        If current_user and user_filter_property are set, only return rows where the user_filter_property
        matches the current_user's email.
        """
        result = await self.read(db, filters, sort_field, sort_order, current_user=current_user, approved_only=approved_only)
        return result[0] if len(result) > 0 else None


    async def read_by_id(
        self,
        db: AsyncSession,
        ident: int | str,
        current_user: Optional[AuthorizedUser] = None,
        bypass_user_filter: bool = False,
        approved_only: bool = False,
    ) -> Optional[_ModelVar]:
        """
        Read a single row from the database by its ID.
        If current_user and user_filter_property are set, only return rows where the user_filter_property
        matches the current_user's email.
        """
        filters: FiltersType = {
            "id": ident,
            "bypass_user_filter": bypass_user_filter,
        }
        
        if approved_only and self.is_core_model:
            filters["approved"] = True
        
        results = await self.read(
            db,
            filters=filters,
            current_user=current_user,
        )
        return results[0] if results else None

    async def read_by_id_request(
        self,
        db: AsyncSession,
        request: Request,
        ident: int | str,
    ) -> Optional[_ModelVar]:
        """
        Read a single row from the database by its ID.
        """
        current_user: AuthorizedUser = request.state.current_user
        bypass_user_filter = request.query_params.get("bypass_user_filter", False) != False
        
        return await self.read_by_id(db, ident, current_user, bypass_user_filter=bypass_user_filter)

    async def read_request(
        self,
        db: AsyncSession,
        request: Request,
        additional_filters: Optional[FiltersType] = None,
    ) -> list[_ModelVar]:
        """
        Read all rows from the database that match the filters given by the request.
        Optionally sort the results, if a sort field is provided in the request.
        """
        filters, sort_field, sort_order, limit = await self.get_sort_and_filter_from_request(
            request
        )
        return await self.read(
            db,
            filters={**filters, **(additional_filters if additional_filters else {})},
            sort_field=sort_field,
            sort_order=sort_order,
            limit=limit,
            current_user=request.state.current_user,
        )
        
    async def paginate(
        self,
        db: AsyncSession,
        filters: Optional[FiltersType] = None,
        sort_field: Optional[str] = None,
        sort_order: Optional[SortOrder] = None,
        current_user: Optional[AuthorizedUser] = None,
    ) -> Page[_ReadSchemaVar]:
        """
        Fetch a paginated list of rows from the database that match the given filters.
        Optionally sort the results.
        If current_user and user_filter_property are set, only return rows where the user_filter_property
        matches the current_user's email.
        """
        filters = filters or {}
        
        query = select(self.model_type)
        query = self.apply_select_options(query)

        # filtering
        query = self._apply_filters(CrudOperation.READ, query, filters, current_user)

        # sorting
        if sort_field is not None and sort_order is not None:
            query = apply_sort(sort_field, sort_order, query, self.model_type)
        else:
            query = self.apply_default_ordering(query) # type: ignore

        return await paginate(conn=db, query=query)

    async def paginate_request(
        self,
        db: AsyncSession,
        request: Request,
    ) -> Page[_ReadSchemaVar]:
        """
        Fetch a paginated list of rows from the database that match the filters given by the request.
        Optionally sort the results, if a sort field is provided in the request.
        """
        filters, sort_field, sort_order, _ = await self.get_sort_and_filter_from_request(request)
        current_user: AuthorizedUser = request.state.current_user

        return await self.paginate(db, filters, sort_field, sort_order, current_user)
    
    
    async def find_page(
        self,
        db: AsyncSession,
        item_id: str | int,
        page_size: int,
    ) -> int:
        """
        Given an Item ID, determine the page number it is on based on default sorting and filtering.
        Currently unused.
        """
        query = select(self.model_type)
        
        id_column: InstrumentedAttribute = getattr(self.model_type, "id")
        query = self.apply_select_options(query).with_only_columns(id_column)
        
        query = self._apply_filters(CrudOperation.READ, query, {})
        query = apply_sort("id", SortOrder.ASC, query, self.model_type)

        results = list((await db.execute(query)).scalars().all())
        ids = [getattr(result, "id") for result in results]

        return ids.index(item_id) // page_size + 1

    # ---------- Count ----------

    async def count(
        self,
        db: AsyncSession,
        filters: Optional[FiltersType] = None,
        current_user: Optional[AuthorizedUser] = None,
    ) -> int:
        """
        Count all rows from the database that match the given filters.
        If current_user and user_filter_property are set, only return rows where the user_filter_property
        matches the current_user's email.
        """
        filters = filters or {}
        
        id_property: InstrumentedAttribute = getattr(self.model_type, "id")
        query = select(func.count(id_property))

        # filtering
        query = self._apply_filters(CrudOperation.READ, query, filters, current_user)

        return int((await db.scalar(query)) or 0)


    async def count_request(
        self,
        db: AsyncSession,
        request: Request,
        additional_filters: Optional[FiltersType] = None,
    ) -> int:
        """
        Count all rows from the database that match the filters given by the request.
        """
        filters, _, _, _ = await self.get_sort_and_filter_from_request(request)
        return await self.count(
            db,
            filters={**filters, **(additional_filters if additional_filters else {})},
            current_user=request.state.current_user,
        )

    # ---------- Update ----------

    async def update(
        self,
        db: AsyncSession,
        filters: FiltersType,
        data: _UpdateSchemaVar,
        current_user: Optional[AuthorizedUser] = None,
    ) -> list[_ModelVar]:
        """
        Update all rows in the database that match the given filters.
        """
        query = select(self.model_type)
        query = self._apply_filters(CrudOperation.UPDATE, query, filters, current_user)

        assert isinstance(data, schemas.BaseModel)

        results: list[_ModelVar] = list((await db.execute(query)).scalars().all())
        ids = [getattr(result, "id") for result in results]

        for id in ids:
            results.append(await self.update_by_id(db, id, data, current_user))

        return results

    async def update_by_id(
        self,
        db: AsyncSession,
        ident: int | str,
        data: _UpdateSchemaVar,
        current_user: Optional[AuthorizedUser] = None,
    ) -> _ModelVar:
        """
        Update a row in the database by its ID. This function is called by all update functions and should
        be overridden by subclasses if additional logic is needed.

        If current_user and user_filter_property are set, only allow the update if the user_filter_property
        matches the current_user's email.
        """
        db_value = await self.read_by_id(db, ident)
        if db_value is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Value not found")

        if self.user_filter_property:
            if current_user and not current_user.is_admin:
                if getattr(db_value, self.user_filter_property) != current_user.email:
                    raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden")

        assert isinstance(data, schemas.BaseModel)

        for key, value in data.model_dump(exclude_unset=True).items():
            setattr(db_value, key, value)

        return db_value

    async def update_request(
        self,
        db: AsyncSession,
        request: Request,
        data: _UpdateSchemaVar,
    ) -> list[_ModelVar]:
        """
        Update all rows in the database that match the filters given by the request.
        """
        filters, _, _, _ = await self.get_sort_and_filter_from_request(request)
        current_user: AuthorizedUser = request.state.current_user
        return await self.update(db, filters, data, current_user)

    async def update_by_id_request(
        self,
        db: AsyncSession,
        request: Request,
        ident: int | str,
        data: _UpdateSchemaVar,
    ) -> _ModelVar:
        """
        Update a row in the database by its ID, using the data provided in the request.
        """
        current_user: AuthorizedUser = request.state.current_user
        return await self.update_by_id(db, ident, data, current_user)


    # ---------- Approve ----------

    async def approve(
        self,
        db: AsyncSession,
        filters: FiltersType,
        current_user: Optional[AuthorizedUser] = None,
    ) -> list[_ModelVar]:
        """
        Approve all rows in the database that match the given filters.
        """
        assert self.is_core_model, "Only core models can be approved"
        
        query = select(self.model_type)
        query = self._apply_filters(CrudOperation.UPDATE, query, filters, current_user)

        results: list[_ModelVar] = list((await db.execute(query)).scalars().all())
        ids = [getattr(result, "id") for result in results]

        for id in ids:
            results.append(await self.approve_by_id(db, id, current_user))

        return results
    
    async def approve_by_id(
        self,
        db: AsyncSession,
        ident: int | str,
        current_user: Optional[AuthorizedUser] = None,
    ) -> _ModelVar:
        db_value = await self.read_by_id(db, ident)
        if db_value is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Value not found")

        setattr(db_value, "approved", True)
        await db.flush()
        await db.refresh(db_value)
        
        return db_value
    
    
    async def revoke_approval(
        self,
        db: AsyncSession,
        filters: FiltersType,
        current_user: Optional[AuthorizedUser] = None,
    ) -> list[_ModelVar]:
        """
        Revoke approval for all rows in the database that match the given filters.
        """
        assert self.is_core_model, "Only core models can be approved"
        
        query = select(self.model_type)
        query = self._apply_filters(CrudOperation.UPDATE, query, filters, current_user)

        results: list[_ModelVar] = list((await db.execute(query)).scalars().all())
        ids = [getattr(result, "id") for result in results]

        for id in ids:
            results.append(await self.revoke_approval_by_id(db, id, current_user))

        return results
    
    async def revoke_approval_by_id(
        self,
        db: AsyncSession,
        ident: int | str,
        current_user: Optional[AuthorizedUser] = None,
    ) -> _ModelVar:
        """
        Revoke approval for a row in the database by its ID.
        """
        db_value = await self.read_by_id(db, ident)
        if db_value is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Value not found")

        setattr(db_value, "approved", False)
        await db.flush()
        await db.refresh(db_value)
        
        return db_value
    
    
    # ---------- Delete ----------

    async def delete(
        self,
        db: AsyncSession,
        filters: FiltersType,
        current_user: Optional[AuthorizedUser] = None,
    ) -> int:
        """
        Deletes all rows in the database that match the given filters.
        Attention: When custom delete logic is needed, both delete and delete_by_id need to be overridden.
        """
        query = select(self.model_type)
        query = self._apply_filters(CrudOperation.DELETE, query, filters, current_user)

        results = (await db.execute(query)).scalars().all()
        ids = [getattr(result, "id") for result in results]

        for id in ids:
            await self.delete_by_id(db, id, current_user)

        return len(results)

    async def delete_by_id(
        self,
        db: AsyncSession,
        ident: int | str,
        current_user: Optional[AuthorizedUser] = None,
    ) -> _ReadSchemaVar:
        """
        Delete a row in the database by its ID. This function is called by all delete functions except
        `delete_unchecked` and should be overridden by subclasses if additional logic is needed.

        If current_user and user_filter_property are set, only allow the delete if the user_filter_property
        matches the current_user's email.
        """
        db_value = await self.read_by_id(db, ident)
        if db_value is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Value not found")

        read_value = self.read_type.model_validate(db_value)

        if self.user_filter_property:
            if current_user and not current_user.is_admin:
                if getattr(db_value, self.user_filter_property) != current_user.email:
                    raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden")

        await db.delete(db_value)
        
        return read_value

    async def delete_all(self, db: AsyncSession) -> int:
        """
        Delete all rows in the database.
        """
        return await self.delete(db, filters={})

    async def delete_request(
        self,
        db: AsyncSession,
        request: Request,
    ) -> int:
        """
        Deletes all rows in the database that match the filters given by the request.
        """
        filters, _, _, _ = await self.get_sort_and_filter_from_request(request)
        current_user: AuthorizedUser = request.state.current_user
        return await self.delete(db, filters, current_user)

    async def delete_by_id_request(
        self,
        db: AsyncSession,
        request: Request,
        ident: int | str,
    ):
        """
        Delete a row in the database by its ID.
        """
        current_user: AuthorizedUser = request.state.current_user
        await self.delete_by_id(db, ident, current_user)

    async def delete_unchecked(
        self,
        db: AsyncSession,
        ident: int | str,
        current_user: Optional[AuthorizedUser] = None,
    ):
        """
        Deletes the row with the given identifier, without calling the event listeners.
        """
        db_value = await self.read_by_id(db, ident)
        if db_value is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Value not found")

        if self.user_filter_property:
            if current_user and not current_user.is_admin:
                if getattr(db_value, self.user_filter_property) != current_user.email:
                    raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden")

        await db.delete(db_value)

    async def bulk_delete_unchecked(
        self,
        db: AsyncSession,
        filters: FiltersType,
        current_user: Optional[AuthorizedUser] = None,
    ) -> int:
        """
        Deletes all rows in the database that match the given filters in a bulk operation,
        without calling the event listeners.
        """
        query = delete(self.model_type)
        query: Delete = self._apply_filters(CrudOperation.DELETE, query, filters, current_user)

        return (await db.execute(query)).rowcount

    # ---------- Validation ----------
    
    async def validate_create(
        self,
        db: AsyncSession,
        request_data: _ValidateCreateSchemaVar,
        initial_relationships: list[schemas.ModifiedRelationship],
    ) -> _CreateSchemaVar:
        """
        Validate the data provided in a create request.
        """
        await self.validate_relationships_create(db, initial_relationships)
        return self.create_type.model_validate(request_data.model_dump(exclude_unset=True))
    
    
    async def validate_update(
        self,
        db: AsyncSession,
        id: int | str,
        request_data: _ValidateUpdateSchemaVar,
        modified_relationships: list[schemas.ModifiedRelationship],
    ) -> _UpdateSchemaVar:
        """
        Validate the data provided in an update request.
        """
        await self.validate_relationships_update(db, modified_relationships)
        return self.update_type.model_validate(request_data.model_dump(exclude_unset=True))
    
    
    async def validate_relationships_create(
        self,
        db: AsyncSession,
        initial_relationships: list[schemas.ModifiedRelationship],
    ):
        """
        Validate the relationships provided in a create request.
        """
        required_relationships = [r for r in self.relationships if r.required]
        for relationship in required_relationships:
            initial_rel = next((r for r in initial_relationships if r.relationship_id == relationship.id), None)
            if initial_rel is None:
                if relationship.foreign_key_column:
                    raise CustomValidationError(
                        type="required_relationship",
                        message=f"Relationship {relationship.id} is required",
                        property=relationship.foreign_key_column,
                        input="",
                    )
                
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Relationship {relationship.id} is required"
                )
                
            match relationship.type:
                case types.ModelRelationshipType.ONE_TO_ONE | types.ModelRelationshipType.MANY_TO_ONE:
                    if len(initial_rel.new_value_ids) != 1 and relationship.required:
                        raise CustomValidationError(
                            type="invalid_relationship",
                            message=f"Relationship {relationship.id} expects exactly one value, but {len(initial_rel.new_value_ids)} were provided",
                            property=relationship.foreign_key_column or "<missing>",
                            input="",
                        )
                case _:
                    pass
    
    
    async def validate_relationships_update(
        self,
        db: AsyncSession,
        modified_relationships: list[schemas.ModifiedRelationship],
    ):
        """
        Validate the relationships provided in a create request.
        """
        for modified_rel in modified_relationships:
            relationship = self.find_relationship(id=modified_rel.relationship_id)
            if not relationship:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Relationship {modified_rel.relationship_id} not found"
                )
                
            match relationship.type:
                case types.ModelRelationshipType.ONE_TO_ONE | types.ModelRelationshipType.MANY_TO_ONE:
                    if len(modified_rel.new_value_ids) != 1 and relationship.required:
                        raise CustomValidationError(
                            type="invalid_relationship",
                            message=f"Relationship {relationship.id} expects exactly one value, but {len(modified_rel.new_value_ids)} were provided",
                            property=relationship.foreign_key_column or "<missing>",
                            input="",
                        )
                case _:
                    pass
    
    # ---- Modification Requests ----
    
    def _update_foreign_key_relationships(
        self,
        request: schemas.ModificationRequestRead,
        model: schemas.BaseModel,
    ):
        for modified_rel in request.modified_relationships:
            relationship = self.find_relationship(modified_rel.relationship_id)
            assert relationship is not None, "Relationship not found"
            
            if relationship.foreign_key_column is None:
                continue
            
            setattr(model, relationship.foreign_key_column, modified_rel.new_value_ids[0] if len(modified_rel.new_value_ids) > 0 else None)
    
    async def _update_x_to_many_relationships(
        self,
        db: AsyncSession,
        request: schemas.ModificationRequestRead,
        value_id: int,
    ):
        for modified_rel in request.modified_relationships:
            relationship = self.find_relationship(modified_rel.relationship_id)
            assert relationship is not None, "Relationship not found"
            
            current_value_ids = await self.get_relationship_ids(
                db, relationship, value_id
            )
            
            added_value_ids = set(modified_rel.new_value_ids) - set(current_value_ids)
            removed_value_ids = set(current_value_ids) - set(modified_rel.new_value_ids)
            
            if relationship.reverse_foreign_key_column is not None:
                related_controller = CrudController.get_crud_controller(relationship.table_name)
                assert related_controller is not None, f"CRUD controller for {relationship.table_name} not found"
                
                # add new values
                for related_value_id in added_value_ids:
                    update_data = {
                        relationship.reverse_foreign_key_column: value_id,
                    }
                    
                    await related_controller.update_by_id(
                        db, related_value_id, related_controller.update_type(**update_data)
                    )
                    
                # remove old values
                for related_value_id in removed_value_ids:
                    update_data = {
                        relationship.reverse_foreign_key_column: None,
                    }
                    
                    await related_controller.update_by_id(
                        db, related_value_id, related_controller.update_type(**update_data)
                    )
            
            if relationship.connection_table_name is not None:
                for related_value_id in added_value_ids:
                    await self.add_value_to_many_to_many_relationship(
                        db, relationship, value_id, related_value_id
                    )
                    
                for related_value_id in removed_value_ids:
                    await self.remove_value_from_many_to_many_relationship(
                        db, relationship, value_id, related_value_id
                    )
                    
                    
    async def _remove_x_to_many_relationships(
        self,
        db: AsyncSession,
        request: schemas.ModificationRequestRead,
    ):
        assert request.value_id is not None, "value_id is required"
        
        for relationship in self.relationships:
            if relationship.reverse_foreign_key_column is not None:
                ids = await self.get_relationship_ids(db, relationship, request.value_id)
            
                related_controller = CrudController.get_crud_controller(relationship.table_name)
                assert related_controller is not None, f"CRUD controller for {relationship.table_name} not found"
                
                for related_value_id in ids:
                    update_data = {
                        relationship.reverse_foreign_key_column: None,
                    }
                    
                    await related_controller.update_by_id(
                        db, related_value_id, related_controller.update_type(**update_data)
                    )
            
            if relationship.connection_table_name is not None:
                ids = await self.get_relationship_ids(db, relationship, request.value_id)
            
                for related_value_id in ids:
                    await self.remove_value_from_many_to_many_relationship(
                        db, relationship, request.value_id, related_value_id
                    )
    
    
    async def create_change_history_entry_for_create(
        self,
        db: AsyncSession,
        approval_date: datetime,
        request: schemas.ModificationRequestRead,
        value_id: int,
        details_json: str,
        change_crud: CrudController,
    ):
        """
        Create a change history entry for a CREATE operation.
        """
        await change_crud.create(db, schemas.ChangeHistoryEntryCreate(
            automatically_generated=False,
            operation=types.ModificationRequestOperation.CREATE,
            approval_date=approval_date,
            table_name=self.model_type.__tablename__,
            value_id=value_id,
            property_key=None,
            old_value=None,
            new_value=details_json,
            modification_request_id=request.id,
        ))
    
    
    async def create_sharepoint_migrations_for_create(
        self,
        db: AsyncSession,
        migration_date: datetime,
        request: schemas.ModificationRequestRead,
        value: _ReadSchemaVar,
    ):
        """
        Create sharepoint_migration entries for a CREATE operation.
        By default, this method does nothing. It can be overridden by subclasses if they need to create
        sharepoint_migration entries.
        """
        pass
    
    
    async def execute_create_request(
        self,
        db: AsyncSession,
        request: schemas.ModificationRequestRead,
    ) -> _ReadSchemaVar:
        """
        Execute a modification request to create a new object in the database.
        """
        # the value was already created in unapproved state when the request was
        # opened, so here we just need to approve it
        new_value_id = request.value_id
        assert new_value_id is not None, "value_id is required"
        
        # mark the value as approved
        db_read_value = await self.approve_by_id(db, new_value_id)
        read_value = self.read_type.model_validate(db_read_value)
        
        # create the change history entry
        change_crud = self.get_crud_controller("change_history")
        assert change_crud is not None, "Change history CRUD not found"
        
        approval_date=datetime.now(timezone.utc).replace(tzinfo=None)
        
        await self.create_change_history_entry_for_create(
            db, approval_date, request, new_value_id, read_value.model_dump_json(), change_crud,
        )
        
        # create sharepoint_migration entries if needed
        await self.create_sharepoint_migrations_for_create(db, approval_date, request, read_value)
        
        return read_value
    
    
    async def create_change_history_entry_for_update(
        self,
        db: AsyncSession,
        approval_date: datetime,
        request: schemas.ModificationRequestRead,
        property_key: str,
        old_value: Any,
        new_value: Any,
        change_crud: CrudController,
        new_value_summary: Optional[str] = None,
    ):
        """
        Create a change history entry for an UPDATE operation.
        """
        await change_crud.create(db, schemas.ChangeHistoryEntryCreate(
            automatically_generated=False,
            operation=types.ModificationRequestOperation.UPDATE,
            approval_date=approval_date,
            table_name=self.model_type.__tablename__,
            value_id=request.value_id,
            property_key=property_key,
            old_value=f"{old_value}",
            new_value=f"{new_value}",
            new_value_summary=new_value_summary,
            modification_request_id=request.id,
        ))
    
    
    async def create_sharepoint_migrations_for_update(
        self,
        db: AsyncSession,
        migration_date: datetime,
        request: schemas.ModificationRequestRead,
        old_value: _ReadSchemaVar,
        new_value: _ReadSchemaVar,
    ):
        """
        Create sharepoint_migration entries for an UPDATE operation.
        By default, this method does nothing. It can be overridden by subclasses if they need to create
        sharepoint_migration entries.
        """
        pass
    
    
    async def execute_update_request(
        self,
        db: AsyncSession,
        request: schemas.ModificationRequestRead,
    ) -> _ReadSchemaVar:
        """
        Execute a modification request to update an object in the database.
        """
        assert request.value_id, "value_id is required"
        
        # validate the data
        request_data: dict[str, Any] = request.details
        update_model = self.update_type.model_validate(request_data, from_attributes=True)
        
        # create the change history entry
        change_crud = self.get_crud_controller("change_history")
        assert change_crud is not None, "Change history CRUD not found"
        
        approval_date=datetime.now(timezone.utc).replace(tzinfo=None)
        
        # find the current value
        db_current_value = await self.read_by_id(db, request.value_id)
        assert db_current_value is not None, "Value not found"
        current_value_read = self.read_type.model_validate(db_current_value)
        
        # create change history entries for relationships
        changed_values: dict[str, tuple[Any, Any]] = {}
        for modified_rel in request.modified_relationships:
            relationship = next((rel for rel in self.relationships if rel.id == modified_rel.relationship_id), None)
            if relationship is None:
                logger.warning(f"Relationship with ID {modified_rel.relationship_id} not found")
                continue
            
            match relationship.type:
                case types.ModelRelationshipType.ONE_TO_ONE | types.ModelRelationshipType.MANY_TO_ONE:
                    if relationship.foreign_key_column is None:
                        logger.warning("Invalid one-to-one relationship modification")
                        continue
                    
                    if len(modified_rel.new_value_ids) == 0:
                        new_value_id = None
                    elif len(modified_rel.new_value_ids) == 1:
                        new_value_id = modified_rel.new_value_ids[0]
                    else:
                        logger.warning("Invalid one-to-one relationship modification")
                        continue
                    
                    old_value_id = getattr(current_value_read, relationship.foreign_key_column)
                    
                    summary = await self.generate_summary_for_x_to_one_relationship(
                        db, relationship, new_value_id
                    )
                    
                    await self.create_change_history_entry_for_update(
                        db, approval_date, request, relationship.foreign_key_column, old_value_id, new_value_id,
                        change_crud, new_value_summary=summary,
                    )
                    
                    changed_values[relationship.foreign_key_column] = (old_value_id, new_value_id)
                
                case types.ModelRelationshipType.MANY_TO_MANY:
                    if relationship.connection_table_name is None:
                        logger.warning("Invalid many-to-many relationship modification")
                        continue
                    
                    connection_crud = CrudController.get_crud_controller(relationship.connection_table_name)
                    assert connection_crud is not None, f"CRUD controller for {relationship.connection_table_name} not found"
                    
                    self_foreign_key_column = relationship.connection_foreign_key_column_self or f"{self.table_name}_id"
                    related_foreign_key_column = relationship.connection_foreign_key_column_related or f"{relationship.table_name}_id"
                    
                    db_current_connections = await connection_crud.read(db, {
                        self_foreign_key_column: request.value_id,
                    })
                    
                    current_connection_ids: set[int] = set(getattr(db_connection, related_foreign_key_column) for db_connection in db_current_connections)
                    new_connection_ids = modified_rel.new_value_ids
                    
                    summary = await self.generate_summary_for_x_to_many_relationship(
                        db, relationship, new_connection_ids
                    )
                    
                    await self.create_change_history_entry_for_update(
                        db, approval_date, request, relationship.connection_table_name,
                        ",".join([str(id) for id in current_connection_ids]),
                        ",".join([str(id) for id in new_connection_ids]), 
                        change_crud, new_value_summary=summary,
                    )
                    
                    changed_values[relationship.connection_table_name] = (current_connection_ids, new_connection_ids)
            
            
        # compare the values and create change history entries for each change
        for key in request_data:
            if key in changed_values:
                continue
            if key == "id" or not hasattr(current_value_read, key):
                continue
            
            old_value = getattr(current_value_read, key)
            new_value = request_data[key]
            
            if old_value == new_value:
                continue
            
            await self.create_change_history_entry_for_update(
                db, approval_date, request, key, old_value, new_value, change_crud
            )
            
            changed_values[key] = (old_value, new_value)
        
        # update foreign key relationships
        self._update_foreign_key_relationships(request, update_model)
        
        # update the object
        db_value = await self.update_by_id(db, request.value_id, update_model)
        read_value = self.read_type.model_validate(db_value)
        
        # update x-to-many relationships
        await self._update_x_to_many_relationships(db, request, request.value_id)
        
        # create sharepoint_migration entries if needed
        await self.create_sharepoint_migrations_for_update(
            db, approval_date, request, current_value_read, read_value
        )
        
        return read_value
    
    
    async def create_change_history_entry_for_delete(
        self,
        db: AsyncSession,
        approval_date: datetime,
        request: schemas.ModificationRequestRead,
        change_crud: CrudController,
        deleted_value: _ReadSchemaVar,
    ):
        """
        Create a change history entry for a DELETE operation.
        """
        await change_crud.create(db, schemas.ChangeHistoryEntryCreate(
            automatically_generated=False,
            operation=types.ModificationRequestOperation.DELETE,
            approval_date=approval_date,
            table_name=self.model_type.__tablename__,
            value_id=request.value_id,
            property_key=None,
            old_value=deleted_value.model_dump_json(),
            new_value=None,
            modification_request_id=request.id,
        ))
    
    
    async def create_sharepoint_migrations_for_delete(
        self,
        db: AsyncSession,
        migration_date: datetime,
        request: schemas.ModificationRequestRead,
        deleted_value: _ReadSchemaVar,
    ):
        """
        Create sharepoint_migration entries for a DELETE operation.
        By default, this method does nothing. It can be overridden by subclasses if they need to create
        sharepoint_migration entries.
        """
        pass
    
    
    async def execute_delete_request(
        self,
        db: AsyncSession,
        request: schemas.ModificationRequestRead,
    ) -> _ReadSchemaVar:
        """
        Execute a modification request to delete an object in the database.
        """
        assert request.value_id is not None, "value_id is required"
        
        # remove x-to-many relationships
        await self._remove_x_to_many_relationships(db, request)
        
        # delete the value
        deleted_value = await self.delete_by_id(db, request.value_id)
        
        # create the change history entry
        change_crud = self.get_crud_controller("change_history")
        assert change_crud is not None, "Change history CRUD not found"
        
        approval_date=datetime.now(timezone.utc).replace(tzinfo=None)
        await self.create_change_history_entry_for_delete(
            db, approval_date, request, change_crud, deleted_value,
        )
        
        # create sharepoint_migration entries if needed
        await self.create_sharepoint_migrations_for_delete(db, approval_date, request, deleted_value)
        
        return deleted_value
    
    
    # ------- Relationships -------
    
    @staticmethod
    def _check_relationship_definitions(model_name: str, relationships: list[schemas.ModelRelationship]):
        """
        Verify that the relationship IDs match the model name and that the required columns are set.
        """
        relationship_ids: set[str] = set()
        for relationship in relationships:
            assert relationship.id not in relationship_ids, f"Duplicate relationship ID {relationship.id}"
            relationship_ids.add(relationship.id)
            
            assert relationship.id.startswith(f"{model_name}."), f"Relationship ID {relationship.id} does not match model name {model_name}"
            
            match relationship.type:
                case types.ModelRelationshipType.ONE_TO_ONE | types.ModelRelationshipType.MANY_TO_ONE:
                    assert relationship.foreign_key_column is not None, "relationship ID could not be created"
                case types.ModelRelationshipType.MANY_TO_MANY:
                    assert relationship.connection_table_name is not None, "relationship ID could not be created"
                case types.ModelRelationshipType.ONE_TO_MANY:
                    assert relationship.reverse_foreign_key_column is not None, "relationship ID could not be created"
    
    
    @staticmethod
    def _get_inherited_relationships(
        parent_crud: CrudController,
        model_name: str,
    ) -> list[schemas.ModelRelationship]:
        """
        Adjust the relationships of the crud controller this class inherits from to
        match the subclass model name.
        """
        inherited_relationships: list[schemas.ModelRelationship] = []
        for relationship in parent_crud.relationships:
            match relationship.type:
                case types.ModelRelationshipType.ONE_TO_ONE:
                    assert relationship.foreign_key_column is not None, "foreign_key_column is required"
                    inherited_relationships.append(schemas.ModelRelationship.one_to_one(
                        model_name=model_name,
                        related_table_name=relationship.table_name,
                        foreign_key_column=relationship.foreign_key_column,
                        reverse_foreign_key_column=relationship.reverse_foreign_key_column,
                        required=relationship.required,
                    ))
                case types.ModelRelationshipType.MANY_TO_ONE:
                    assert relationship.foreign_key_column is not None, "foreign_key_column is required"
                    inherited_relationships.append(schemas.ModelRelationship.many_to_one(
                        model_name=model_name,
                        related_table_name=relationship.table_name,
                        foreign_key_column=relationship.foreign_key_column,
                        required=relationship.required,
                    ))
                case types.ModelRelationshipType.ONE_TO_MANY:
                    assert relationship.reverse_foreign_key_column is not None, "reverse_foreign_key_column is required"
                    inherited_relationships.append(schemas.ModelRelationship.one_to_many(
                        model_name=model_name,
                        related_table_name=relationship.table_name,
                        reverse_foreign_key_column=relationship.reverse_foreign_key_column,
                        required=relationship.required,
                    ))
                case types.ModelRelationshipType.MANY_TO_MANY:
                    assert relationship.connection_table_name is not None, "connection_table_name is required"
                    inherited_relationships.append(schemas.ModelRelationship.many_to_many(
                        model_name=model_name,
                        related_table_name=relationship.table_name,
                        connection_table_name=relationship.connection_table_name,
                        connection_foreign_key_column_self=relationship.connection_foreign_key_column_self,
                        connection_foreign_key_column_related=relationship.connection_foreign_key_column_related,
                        required=relationship.required,
                    ))
        
        return inherited_relationships
    
    
    async def get_relationship_ids(
        self,
        db: AsyncSession,
        relationship: schemas.ModelRelationship,
        value_id: int | str,
    ) -> list[int]:
        """
        Get the IDs of related objects.
        """
        db_value = await self.read_by_id(db, value_id)
        assert db_value is not None, "Value not found"
        
        crud_controller = CrudController.get_crud_controller(relationship.table_name)
        assert crud_controller is not None, f"CRUD controller for {relationship.table_name} not found"
        
        if (
            relationship.type == types.ModelRelationshipType.ONE_TO_ONE 
            or relationship.type == types.ModelRelationshipType.MANY_TO_ONE
        ):
            assert relationship.foreign_key_column is not None, "foreign_key_column is required"
            
            foreign_key = getattr(db_value, relationship.foreign_key_column)
            if foreign_key is None:
                if not relationship.required:
                    return []
                
                assert False, "Foreign key not found"
            
            return [foreign_key]
        
        if relationship.connection_table_name is None:
            # for one-to-many relationships, the relationship may be defined via a back reference
            assert relationship.reverse_foreign_key_column is not None, "reverse_foreign_key_column is required"
            
            db_related_values = await crud_controller.read(db, {
                relationship.reverse_foreign_key_column: value_id,
            })
            
            db_related_value_ids = [getattr(db_related_value, "id") for db_related_value in db_related_values]
        else:
            # for many-to-many relationships, the relationship is always defined via a connection table
            connection_crud_controller = CrudController.get_crud_controller(relationship.connection_table_name)
            assert connection_crud_controller is not None, f"CRUD controller for {relationship.connection_table_name} not found"
            
            self_foreign_key_column = relationship.connection_foreign_key_column_self or f"{self.table_name}_id"
            related_foreign_key_column = relationship.connection_foreign_key_column_related or f"{crud_controller.table_name}_id"
            
            db_connection_values = await connection_crud_controller.read(db, {
                self_foreign_key_column: value_id,
            })
            db_related_value_ids = [getattr(db_connection_value, related_foreign_key_column) for db_connection_value in db_connection_values]
            
            # for relationships to the same table, we need to check the reverse connection as well
            if relationship.table_name == self.table_name:
                db_reverse_connection_values = await connection_crud_controller.read(db, {
                    related_foreign_key_column: value_id,
                })
                db_related_value_ids.extend([getattr(db_reverse_connection_value, self_foreign_key_column) for db_reverse_connection_value in db_reverse_connection_values])
            
            
        
        return db_related_value_ids
    
    
    async def get_related_values(
        self,
        db: AsyncSession,
        relationship: schemas.ModelRelationship,
        value_id: int | str,
    ) -> list[Any]:
        """
        Get the related objects for a relationship.
        """
        crud_controller = CrudController.get_crud_controller(relationship.table_name)
        assert crud_controller is not None, f"CRUD controller for {relationship.table_name} not found"
        
        related_ids = await self.get_relationship_ids(db, relationship, value_id)
        db_related_values = await crud_controller.read(db, {
            "id": related_ids,
        })
        
        return [crud_controller.read_type.model_validate(db_related_value) for db_related_value in db_related_values]
    
    async def generate_relationship_summary(
        self,
        db: AsyncSession,
        relationship: schemas.ModelRelationship,
        value_id: int | str,
    ) -> Optional[str]:
        """
        Generate a summary string for a relationship in this table.
        """
        related_ids = await self.get_relationship_ids(db, relationship, value_id)
        
        if isinstance(related_ids, int):
            return await self.generate_summary_for_x_to_one_relationship(db, relationship, related_ids)
        
        return await self.generate_summary_for_x_to_many_relationship(db, relationship, related_ids)
    
    
    async def generate_summary_for_x_to_one_relationship(
        self,
        db: AsyncSession,
        relationship: schemas.ModelRelationship,
        foreign_key: Optional[int],
    ):
        """
        Generate a summary string for a one-to-one or many-to-one relationship.
        """
        assert relationship.foreign_key_column is not None, "foreign_key_column is required"
        
        if foreign_key is None:
            return ""
        
        crud_controller = CrudController.get_crud_controller(relationship.table_name)
        assert crud_controller is not None, f"CRUD controller for {relationship.table_name} not found"
        
        db_related_value = await crud_controller.read_by_id(db, foreign_key)
        assert db_related_value is not None, "Related value not found"
        
        return crud_controller.generate_value_summary(db_related_value)
    
    
    async def generate_summary_for_x_to_many_relationship(
        self,
        db: AsyncSession,
        relationship: schemas.ModelRelationship,
        foreign_keys: list[int],
    ):
        """
        Generate a summary string for a one-to-many or many-to-many relationship.
        """
        crud_controller = CrudController.get_crud_controller(relationship.table_name)
        assert crud_controller is not None, f"CRUD controller for {relationship.table_name} not found"
        
        db_related_values = await crud_controller.read(db, {
            "id": foreign_keys,
        })
        
        related_values = [crud_controller.read_type.model_validate(db_related_value) for db_related_value in db_related_values]
        related_value_names = sorted([
            crud_controller.generate_value_summary(related_value)
            for related_value in related_values
        ])
        
        summary_str = ""
        while len(related_value_names) > 0:
            next_value = related_value_names.pop(0)
            if len(summary_str) > 0:
                summary_str += ", "
                
            summary_str += next_value
            if len(summary_str) >= 50:
                break
        
        if len(related_value_names) > 0:
            summary_str += f" (+{len(related_value_names)})"
            
        return summary_str
    
    
    def find_relationship(
        self,
        id: Optional[str] = None,
        table_name: Optional[str] = None,
        foreign_key_column: Optional[str] = None,
        connection_table_name: Optional[str] = None,
    ) -> Optional[schemas.ModelRelationship]:
        matching_relationships = [
            rel for rel in self.relationships
            if (
                (id is not None and rel.id == id)
                or (table_name is not None and rel.table_name == table_name)
                or (foreign_key_column is not None and rel.foreign_key_column == foreign_key_column)
                or (connection_table_name is not None and rel.connection_table_name == connection_table_name)
            )
        ]
        
        if len(matching_relationships) == 0:
            return None
        
        assert len(matching_relationships) == 1, "Multiple matching relationships found"
        return matching_relationships[0]
    
    
    async def add_value_to_many_to_many_relationship(
        self,
        db: AsyncSession,
        relationship: schemas.ModelRelationship,
        value_id: int,
        related_value_id: int,
    ):
        """
        Add a value to a many-to-many relationship.
        """
        assert relationship.connection_table_name is not None, "connection_table_name is required"
        
        # make sure both values exist
        if await self.read_by_id(db, value_id) is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Value not found")
        
        related_controller = CrudController.get_crud_controller(relationship.table_name)
        assert related_controller is not None, f"CRUD controller for {relationship.table_name} not found"
        
        if await related_controller.read_by_id(db, related_value_id) is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Related value not found")
        
        connection_crud_controller = CrudController.get_crud_controller(relationship.connection_table_name)
        assert connection_crud_controller is not None, f"CRUD controller for {relationship.connection_table_name} not found"
        
        self_foreign_key_column = relationship.connection_foreign_key_column_self or f"{self.table_name}_id"
        related_foreign_key_column = relationship.connection_foreign_key_column_related or f"{relationship.table_name}_id"
        
        # check if the connection already exists
        existing_connection = await connection_crud_controller.find(db, filters={
            self_foreign_key_column: value_id,
            related_foreign_key_column: related_value_id,
        })
        
        if existing_connection is not None:
            return
            
        # create the connection
        create_data = {
            self_foreign_key_column: value_id,
            related_foreign_key_column: related_value_id,
        }
        create_schema = connection_crud_controller.create_type.model_validate(create_data, from_attributes=True)
        
        await connection_crud_controller.create(db, create_schema)
        
    
    async def remove_value_from_many_to_many_relationship(
        self,
        db: AsyncSession,
        relationship: schemas.ModelRelationship,
        value_id: int,
        related_value_id: int,
    ):
        """
        Remove a value from a many-to-many relationship.
        """
        assert relationship.connection_table_name is not None, "connection_table_name is required"
        
        connection_crud_controller = CrudController.get_crud_controller(relationship.connection_table_name)
        assert connection_crud_controller is not None, f"CRUD controller for {relationship.connection_table_name} not found"
        
        self_foreign_key_column = relationship.connection_foreign_key_column_self or f"{self.table_name}_id"
        related_foreign_key_column = relationship.connection_foreign_key_column_related or f"{relationship.table_name}_id"
        
        connection_values = await connection_crud_controller.read(db, filters={
            self_foreign_key_column: value_id,
            related_foreign_key_column: related_value_id,
        })
        
        for connection_value in connection_values:
            await connection_crud_controller.delete_by_id(db, getattr(connection_value, "id"))
    
    
    # ---------- Utility ----------

    async def get_sort_and_filter_from_request(
        self, request: Request
    ) -> tuple[FiltersType, Optional[str], Optional[SortOrder], Optional[int]]:
        """
        Extract filters, sort field, and sort order from a request.
        """
        # filtering
        filters: FiltersType = {
            k: v
            for k, v in request.query_params.items()
            if k not in ["skip", "limit", "sort", "page", "size"]
        }

        # sorting
        sort = request.query_params.get("sort", None)
        sort_field = None
        sort_order = None

        if sort is not None:
            if "," not in sort:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST, detail="No sort field provided"
                )

            sort_field, sort_order = sort.split(",")
            sort_order = SortOrder.from_string(sort_order)

        # limit
        limit = None
        if "limit" in request.query_params:
            limit = int(request.query_params["limit"])

        return filters, sort_field, sort_order, limit

    # ---------- Search ----------

    async def search(
        self,
        db: AsyncSession,
        query_str: str,
        filters: Optional[FiltersType] = None,
        limit: Optional[int] = None,
    ) -> schemas.SearchGroup[_ReadSchemaVar]:
        if query_str == "": return schemas.SearchGroup[self.read_type](type=self.model_type.__tablename__, values=[])
        
        search_query = func.plainto_tsquery(TS_VECTOR_LANGUAGE, query_str)
        
        search_vector = getattr(self.model_type, "search_vector", None)
        assert search_vector is not None, f"search_vector property not found on {self.model_type.__name__}"

        ranking = func.ts_rank(search_vector, search_query, TS_RANK_NORMALIZATION)

        # if extra search fields are provided, then we perform a substring matching (checks if a string is a substring of the other) on them and rank them by similarity
        # this is useful for small fields like names of models etc.
        max_similarity = None
        if len(self.substring_search_fields) > 0:
            # the similarity function performs trigram matching (see https://www.postgresql.org/docs/current/pgtrgm.html)
            # From the docs: It returns a number that indicates how similar the two arguments are. The range of the result is zero (indicating that the two strings are completely dissimilar) to one (indicating that the two strings are identical).
            # the similarity function is not case sensitive
            max_similarity = func.greatest(
                *[func.similarity(field, query_str) for field in self.substring_search_fields]
            )

        select_query = self.apply_select_options(select(self.model_type))
        if filters:
            select_query = apply_filters(filters, select_query, self.model_type)
        
        query = (
            select_query
            # we combine both here to simplify the ranking value by combining both measures into one
            .add_columns(.9 * ranking + .1 * max_similarity if max_similarity is not None else ranking)
            .filter(
                # if either the full text search or one of the substring checks on the provided extra fields is true, then the item is included in the search results
                or_(
                    # to speedup the query gin indices are used. Each search vector has a gin index.
                    # to speedup the ilike query, a gin index on the extra search fields could be created as well. This is provided by the pg_trgm extension
                    # https://www.postgresql.org/docs/9.1/textsearch-indexes.html
                    search_vector.op("@@")(search_query),  # postgres full text search
                    *[
                        field.ilike(f"%{query_str}%") for field in self.substring_search_fields
                    ],  # substring check. The ilike function is case insensitive like
                )
            )
        )
        
        if max_similarity is not None:
            query = query.order_by(ranking.desc(), max_similarity.desc())
        else:
            query = query.order_by(ranking.desc())
        
        if limit is not None:
            query = query.limit(limit)

        query_response = await db.execute(query)
        items: list[schemas.SearchItem[_ReadSchemaVar]] = []
        for item in query_response:
            row_obj: _ModelVar = item[0]
            row_ranking: int = item[1]
            data = self.read_type.model_validate(row_obj)
            items.append(schemas.SearchItem[_ReadSchemaVar](data=data, ranking=row_ranking))
        
        return schemas.SearchGroup[_ReadSchemaVar](type=self.model_type.__tablename__, values=items)


    # ---------- Routes ----------
    
    def register_routes(self) -> Optional[APIRouter]:
        """
        Register all CRUD routes for this controller.
        """
        if self.route is None:
            return None
        
        router = APIRouter(prefix=f"/{self.route}", tags=[f"CRUD Routes for {self.table_name}"])
        
        # FastAPI matches routes in the order they are defined, so we need to
        # start from the most specific routes and go to the most general ones.
        
        # ---- Other -----
        self.register_custom_routes(router)
        
        # ---- Relationships ----
        self.register_relationship_routes(router)
        
        # --- Search ---
        self.register_search_routes(router)
        
        # ---- Validate ----
        self.register_validate_routes(router)
        
        # ---- Update ----
        self.register_update_routes(router)
        
        # ---- Create ----
        self.register_create_routes(router)
        
        # ---- Delete ----
        self.register_delete_routes(router)
        
        # ---- Read ----
        self.register_read_routes(router)
        
        return router
    
    def register_read_routes(self, router: APIRouter):
        """
        Register routes for reading data from the database.
        """
        @router.get("/paginated", response_model=Page[self.read_type])
        @requires_auth()
        async def get_paginated_route(
            request: Request,
            db: AsyncSession = Depends(get_db_session),
        ):
            return await self.paginate_request(db, request)
        
        
        @router.get("/count", response_model=int)
        @requires_auth()
        async def count_route(
            request: Request,
            db: AsyncSession = Depends(get_db_session),
        ):
            return await self.count_request(db, request)

        
        @router.get("/raw", response_model=list[dict[str, Any]])
        @requires_auth([types.UserRoles.ROLE_ADMIN])
        async def raw_table_data_route(
            request: Request,
            db: AsyncSession = Depends(get_db_session),
        ):
            db_data = await self.read_request(db, request)
            return [{c.name: getattr(r, c.name) for c in r.__table__.columns} for r in db_data]


        @router.get("/{value_id}", response_model=self.read_type)
        @requires_auth()
        async def get_by_id_route(
            request: Request,
            value_id: int | str,
            db: AsyncSession = Depends(get_db_session),
        ):
            # try to convert the value_id to an integer
            if isinstance(value_id, str) and value_id.isdigit():
                value_id = int(value_id)
            
            result = await self.read_by_id_request(db, request=request, ident=value_id)
            if result is None:
                raise HTTPException(status_code=404, detail=f"{self.table_name} {value_id} not found") # @IgnoreException
            
            return result


        @router.get("/{value_id}/page", response_model=int)
        @requires_auth()
        async def find_page_route(
            request: Request,
            value_id: int | str,
            db: AsyncSession = Depends(get_db_session),
        ):
            # try to convert the value_id to an integer
            if isinstance(value_id, str) and value_id.isdigit():
                value_id = int(value_id)
            
            db_value = await self.read_by_id_request(db, request=request, ident=value_id)
            if db_value is None:
                raise HTTPException(status_code=404, detail=f"{self.table_name} {value_id} not found") # @IgnoreException
            
            page_size = int(request.query_params.get("page_size", 50))
            return await self.find_page(db, item_id=value_id, page_size=page_size)


        @router.get("", response_model=list[self.read_type])
        @requires_auth()
        async def get_all_route(
            request: Request,
            db: AsyncSession = Depends(get_db_session),
        ):
            return await self.read_request(db, request)
        
    
    def register_create_routes(self, router: APIRouter):
        """
        Register routes for creating data without a modification
        request in the database.
        """
        @router.post("/unchecked", response_model=self.read_type)
        @requires_auth(roles=[types.UserRoles.ROLE_ADMIN])
        async def create_route(
            request: Request,
            unchecked_data: dict[str, Any] = Body(...),
            db: AsyncSession = Depends(get_db_session),
        ):
            to_create = self.create_type.model_validate(unchecked_data)

            db_value = self.model_type(**to_create.model_dump())
            if self.is_core_model:
                setattr(db_value, "approved", True)
            
            db.add(db_value)

            await db.flush()
            await db.refresh(db_value)

            result = await self.read_by_id(db, getattr(db_value, "id"))
            assert result is not None, "Value not found"
            
            return result
    
    
    def register_update_routes(self, router: APIRouter):
        """
        Register routes for updating data without a modification
        request in the database.
        """
        @router.patch("/unchecked/{value_id}", response_model=self.read_type)
        @requires_auth(roles=[types.UserRoles.ROLE_ADMIN])
        async def update_by_id_route(
            request: Request,
            value_id: int | str,
            unchecked_data: dict[str, Any] = Body(...),
            db: AsyncSession = Depends(get_db_session),
        ):
            # try to convert the value_id to an integer
            if isinstance(value_id, str) and value_id.isdigit():
                value_id = int(value_id)
            
            db_value = await self.read_by_id_request(db, request=request, ident=value_id)
            if db_value is None:
                raise HTTPException(status_code=404, detail=f"{self.table_name} {value_id} not found") # @IgnoreException
            
            update_model = self.update_type.model_validate(unchecked_data)
            return await self.update_by_id_request(db, request, value_id, update_model)
    
    
    def register_validate_routes(self, router: APIRouter):
        """
        Register routes for validating data before creating or updating it in the database.
        """
        @router.post("/validate", response_model=self.create_type)
        @requires_auth()
        async def validate_create_route(
            request: Request,
            data: dict[str, Any] = Body(...),
            db: AsyncSession = Depends(get_db_session),
        ):
            validate_model = self.validate_create_type.model_validate(data["data"])
            modified_relationships = [schemas.ModifiedRelationship.model_validate(rel) for rel in data["modified_relationships"]]
            
            return await self.validate_create(db, validate_model, modified_relationships)


        @router.patch("/{value_id}/validate", response_model=self.update_type)
        @requires_auth()
        async def validate_update(
            request: Request,
            value_id: int | str,
            data: dict[str, Any] = Body(...),
            db: AsyncSession = Depends(get_db_session),
        ):
            # try to convert the value_id to an integer
            if isinstance(value_id, str) and value_id.isdigit():
                value_id = int(value_id)
            
            db_value = await self.read_by_id_request(db, request=request, ident=value_id)
            if db_value is None:
                raise HTTPException(status_code=404, detail=f"{self.table_name} {value_id} not found") # @IgnoreException
            
            validate_model = self.validate_update_type.model_validate(data["data"])
            modified_relationships = [schemas.ModifiedRelationship.model_validate(rel) for rel in data["modified_relationships"]]
            
            return await self.validate_update(
                db,
                id=value_id, 
                request_data=validate_model,
                modified_relationships=modified_relationships,
            )
    
    
    def register_delete_routes(self, router: APIRouter):
        """
        Register routes for deleting data from the database
        without a modification request.
        """
        @router.delete("/unchecked/{value_id}")
        @requires_auth(roles=[types.UserRoles.ROLE_ADMIN])
        async def delete_by_id_route(
            request: Request,
            value_id: int | str,
            db: AsyncSession = Depends(get_db_session),
        ):
            # try to convert the value_id to an integer
            if isinstance(value_id, str) and value_id.isdigit():
                value_id = int(value_id)
            
            db_value = await self.read_by_id_request(db, request=request, ident=value_id)
            if db_value is None:
                raise HTTPException(status_code=404, detail=f"{self.table_name} {value_id} not found") # @IgnoreException
            
            await self.delete_by_id_request(db, request, value_id)
    
    
    def register_relationship_routes(self, router: APIRouter):
        """
        Register routes for handling relationships between models.
        """
        @router.get("/relationships", response_model=list[schemas.ModelRelationship])
        @requires_auth()
        async def get_relationships_route(
            request: Request,
            db: AsyncSession = Depends(get_db_session),
        ):
            return self.relationships


        @router.get("/relationships/{rel_id}/summary", response_model=dict[int, str])
        @requires_auth()
        async def get_relationship_summaries_route(
            request: Request,
            rel_id: str,
            db: AsyncSession = Depends(get_db_session),
        ):
            relationship = next((rel for rel in self.relationships if rel.id == rel_id), None)
            if relationship is None:
                raise HTTPException(status_code=404, detail=f"Relationship {rel_id} not found") # @IgnoreException

            db_values = await self.read_request(db, request=request)
            summaries: dict[int, str] = {}
            
            for db_value in db_values:
                value_id = getattr(db_value, "id")
                summary = await self.generate_relationship_summary(db, relationship, value_id)
                
                if summary is None:
                    continue
                    
                summaries[value_id] = summary

            return summaries


        @router.get("/{value_id}/relationships/{rel_id}/ids", response_model=list[int])
        @requires_auth()
        async def get_relationship_ids_route(
            request: Request,
            value_id: int | str,
            rel_id: str,
            db: AsyncSession = Depends(get_db_session),
        ):
            # try to convert the value_id to an integer
            if isinstance(value_id, str) and value_id.isdigit():
                value_id = int(value_id)
            
            db_value = await self.read_by_id_request(db, request=request, ident=value_id)
            if db_value is None:
                raise HTTPException(status_code=404, detail=f"{self.table_name} {value_id} not found") # @IgnoreException
            
            relationship = next((rel for rel in self.relationships if rel.id == rel_id), None)
            if relationship is None:
                raise HTTPException(status_code=404, detail=f"Relationship {rel_id} not found")

            return await self.get_relationship_ids(db, relationship, value_id)
        
        
        @router.get("/{value_id}/relationships/{rel_id}/values", response_model=list[Any])
        @requires_auth()
        async def get_relationship_values_route(
            request: Request,
            value_id: int | str,
            rel_id: str,
            db: AsyncSession = Depends(get_db_session),
        ):
            # try to convert the value_id to an integer
            if isinstance(value_id, str) and value_id.isdigit():
                value_id = int(value_id)
            
            db_value = await self.read_by_id_request(db, request=request, ident=value_id)
            if db_value is None:
                raise HTTPException(status_code=404, detail=f"{self.table_name} {value_id} not found") # @IgnoreException
            
            relationship = next((rel for rel in self.relationships if rel.id == rel_id), None)
            if relationship is None:
                raise HTTPException(status_code=404, detail=f"Relationship {rel_id} not found")

            return await self.get_related_values(db, relationship, value_id)
        

        @router.get("/{value_id}/relationships/{rel_id}/summary", response_model=str)
        @requires_auth()
        async def get_relationship_summary_route(
            request: Request,
            value_id: int | str,
            rel_id: str,
            db: AsyncSession = Depends(get_db_session),
        ):
            # try to convert the value_id to an integer
            if isinstance(value_id, str) and value_id.isdigit():
                value_id = int(value_id)
            
            db_value = await self.read_by_id_request(db, request=request, ident=value_id)
            if db_value is None:
                raise HTTPException(status_code=404, detail=f"{self.table_name} {value_id} not found") # @IgnoreException
            
            relationship = next((rel for rel in self.relationships if rel.id == rel_id), None)
            if relationship is None:
                raise HTTPException(status_code=404, detail=f"Relationship {rel_id} not found") # @IgnoreException

            return await self.generate_relationship_summary(db, relationship, value_id)
    
    
    def register_search_routes(self, router: APIRouter):
        """
        Register routes for searching data in this table.
        """
        
        @router.get("/search", response_model=schemas.SearchGroup[self.read_type])
        @requires_auth()
        async def search_route(
            request: Request,
            query: str,
            db: AsyncSession = Depends(get_db_session),
        ):
            filters, _, _, limit = await self.get_sort_and_filter_from_request(request)
            del filters["query"]
            
            return await self.search(db, query, filters, limit)
        
    
    def register_custom_routes(self, router: APIRouter):
        """
        Register custom routes for the CRUD controller.
        Can be overridden by subclasses to add custom routes.
        """
        pass