/*
 * This file is part of lem-sqlite3.
 * Copyright 2012 Emil Renner Berthing
 *
 * lem-sqlite3 is free software: you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * lem-sqlite3 is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with lem-sqlite3.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <stdlib.h>
#include <lem.h>
#include <sqlite3.h>

struct db {
	sqlite3 *handle;
	unsigned int refs;
};

struct box {
	struct db *db;
};
#define db_unbox(T, idx) ((struct box *)lua_touserdata(T, idx))->db

struct stmt {
	sqlite3_stmt *handle;
	struct db *db;
};

static void
db_free(struct db *db)
{
	db->refs--;
	if (db->refs)
		return;

	(void)sqlite3_close(db->handle);
	free(db);
}

static int
stmt_finalize(lua_State *T)
{
	struct stmt *stmt;
	int ret;

	luaL_checktype(T, 1, LUA_TUSERDATA);
	stmt = lua_touserdata(T, 1);
	if (stmt->handle == NULL) {
		lua_pushnil(T);
		lua_pushliteral(T, "already finalized");
		return 2;
	}

	ret = sqlite3_finalize(stmt->handle);
	stmt->handle = NULL;
	db_free(stmt->db);

	if (ret != SQLITE_OK) {
		lua_pushnil(T);
		lua_pushstring(T, sqlite3_errmsg(sqlite3_db_handle(stmt->handle)));
		return 2;
	}
	lua_pushboolean(T, 1);
	return 1;
}

static void
pushrow(lua_State *T, sqlite3_stmt *stmt)
{
	int columns = sqlite3_column_count(stmt);
	int i = 0;

	lua_createtable(T, columns, 0);

	while (i < columns) {
		switch (sqlite3_column_type(stmt, i)) {
		case SQLITE_INTEGER:
		case SQLITE_FLOAT:
			lua_pushnumber(T, sqlite3_column_double(stmt, i));
			break;

		case SQLITE_TEXT:
			lua_pushlstring(T, (char *)sqlite3_column_text(stmt, i),
					(size_t)sqlite3_column_bytes(stmt, i));
			break;

		case SQLITE_BLOB:
			lua_pushlstring(T, (char *)sqlite3_column_blob(stmt, i),
					(size_t)sqlite3_column_bytes(stmt, i));
			break;

		case SQLITE_NULL:
			lua_pushnil(T);
			break;
		}
		lua_rawseti(T, -2, ++i);
	}
}

static int
bindargs(lua_State *T, sqlite3_stmt *stmt)
{
	int top = lua_gettop(T);
	int i;

	for (i = 2; i <= top; i++) {
		int ret;

		switch (lua_type(T, i)) {
		case LUA_TNIL:
			/* Should nil mean NULL..
			ret = sqlite3_bind_null(stmt, i-1);
			break;
			..or don't bind.. */
			continue;

		case LUA_TNUMBER:
			ret = sqlite3_bind_double(stmt, i-1,
					lua_tonumber(T, i));
			break;

		case LUA_TSTRING:
			{
				size_t len;
				const char *str = lua_tolstring(T, i, &len);

				ret = sqlite3_bind_text(stmt, i-1,
						str, len, SQLITE_STATIC);
			}
			break;

		default:
			(void)sqlite3_clear_bindings(stmt);
			return luaL_argerror(T, i, "expected nil, number or string");
		}

		if (ret != SQLITE_OK) {
			(void)sqlite3_clear_bindings(stmt);
			return luaL_argerror(T, i, sqlite3_errmsg(
						sqlite3_db_handle(stmt)));
		}
	}
	return 0;
}

static int
bindtable(lua_State *T, sqlite3_stmt *stmt)
{
	int parameters = sqlite3_bind_parameter_count(stmt);
	const char *name;
	const char *err;
	int i;

	for (i = 1; i <= parameters; i++) {
		int ret;

		name = sqlite3_bind_parameter_name(stmt, i);
		lua_settop(T, 2);
		if (name == NULL || name[0] == '?')
			lua_rawgeti(T, 2, i);
		else if (name[0] == '@')
			lua_getfield(T, 2, name + 1);
		else
			lua_getfield(T, 2, name);

		switch (lua_type(T, 3)) {
		case LUA_TNIL:
			/* Should nil mean NULL..
			ret = sqlite3_bind_null(stmt, i);
			break;
			..or don't bind.. */
			continue;

		case LUA_TNUMBER:
			ret = sqlite3_bind_double(stmt, i, lua_tonumber(T, 3));
			break;

		case LUA_TSTRING:
			{
				size_t len;
				const char *str = lua_tolstring(T, 3, &len);

				ret = sqlite3_bind_text(stmt, i,
						str, len, SQLITE_STATIC);
			}
			break;

		default:
			err = "expected nil, number or string";
			goto error;
		}

		if (ret != SQLITE_OK) {
			err = sqlite3_errmsg(sqlite3_db_handle(stmt));
			goto error;
		}
	}
	return 0;

error:
	(void)sqlite3_clear_bindings(stmt);
	luaL_where(T, 1);
	if (name == NULL || name[0] == '?')
		lua_pushfstring(T, "error binding %d: %s", i, err);
	else
		lua_pushfstring(T, "error binding '%s': %s", name, err);
	lua_concat(T, 2);
	return -1;
}

static int
stmt_bind(lua_State *T)
{
	struct stmt *stmt;

	luaL_checktype(T, 1, LUA_TUSERDATA);
	stmt = lua_touserdata(T, 1);
	if (stmt->handle == NULL) {
		lua_pushnil(T);
		lua_pushliteral(T, "finalized");
		return 2;
	}

	if (lua_type(T, 2) == LUA_TTABLE) {
		if (bindtable(T, stmt->handle))
			return lua_error(T);
		return 0;
	}

	return bindargs(T, stmt->handle);
}

static int
stmt_step(lua_State *T)
{
	struct stmt *stmt;

	luaL_checktype(T, 1, LUA_TUSERDATA);
	stmt = lua_touserdata(T, 1);
	if (stmt->handle == NULL) {
		lua_pushnil(T);
		lua_pushliteral(T, "finalized");
		return 2;
	}

	switch (sqlite3_step(stmt->handle)) {
	case SQLITE_ROW:
		pushrow(T, stmt->handle);
		return 1;

	case SQLITE_DONE:
		(void)sqlite3_reset(stmt->handle);
		(void)sqlite3_clear_bindings(stmt->handle);
		lua_pushboolean(T, 1);
		return 1;
	}

	lua_pushnil(T);
	lua_pushstring(T, sqlite3_errmsg(sqlite3_db_handle(stmt->handle)));
	return 2;
}

static int
stmt_reset(lua_State *T)
{
	struct stmt *stmt;
	int ret;

	luaL_checktype(T, 1, LUA_TUSERDATA);
	stmt = lua_touserdata(T, 1);
	if (stmt->handle == NULL) {
		lua_pushnil(T);
		lua_pushliteral(T, "finalized");
		return 2;
	}

	ret = sqlite3_reset(stmt->handle);
	(void)sqlite3_clear_bindings(stmt->handle);
	if (ret != SQLITE_OK) {
		lua_pushnil(T);
		lua_pushstring(T, sqlite3_errmsg(sqlite3_db_handle(stmt->handle)));
		return 2;
	}

	lua_pushboolean(T, 1);
	return 1;
}

static int
db_prepare(lua_State *T)
{
	struct db *db;
	const char *sql;
	size_t len;
	sqlite3_stmt *handle;
	struct stmt *stmt;

	luaL_checktype(T, 1, LUA_TUSERDATA);
	sql = luaL_checklstring(T, 2, &len);

	db = db_unbox(T, 1);
	if (db == NULL) {
		lua_pushnil(T);
		lua_pushliteral(T, "closed");
		return 2;
	}

	if (sqlite3_prepare_v2(db->handle, sql, len+1, &handle, NULL)
			!= SQLITE_OK) {
		lua_pushnil(T);
		lua_pushstring(T, sqlite3_errmsg(db->handle));
		return 2;
	}

	if (handle == NULL) {
		lua_pushnil(T);
		lua_pushliteral(T, "nosql");
		return 2;
	}

	stmt = lua_newuserdata(T, sizeof(struct stmt));
	stmt->handle = handle;
	stmt->db = db;
	db->refs++;

	/* set metatable */
	lua_pushvalue(T, lua_upvalueindex(1));
	lua_setmetatable(T, -2);

	return 1;
}

static int
db_exec(lua_State *T)
{
	struct db *db;
	const char *sql;
	size_t len;
	unsigned int bind;

	luaL_checktype(T, 1, LUA_TUSERDATA);
	sql = luaL_checklstring(T, 2, &len);
	bind = lua_gettop(T) > 2;
	if (bind) {
		luaL_checktype(T, 3, LUA_TTABLE);
		lua_settop(T, 3);
		lua_replace(T, 2);
	}

	db = db_unbox(T, 1);
	if (db == NULL) {
		lua_pushnil(T);
		lua_pushliteral(T, "closed");
		return 2;
	}

	do {
		sqlite3_stmt *stmt;

		if (sqlite3_prepare_v2(db->handle, sql, len+1, &stmt, &sql)
				!= SQLITE_OK) {
			lua_pushnil(T);
			lua_pushstring(T, sqlite3_errmsg(db->handle));
			return 2;
		}

		if (stmt == NULL)
			break;

		if (bind && bindtable(T, stmt)) {
			(void)sqlite3_finalize(stmt);
			return lua_error(T);
		}

		switch (sqlite3_step(stmt)) {
		case SQLITE_ROW:
		case SQLITE_DONE:
			break;

		default:
			(void)sqlite3_finalize(stmt);
			goto error;
		}

		if (sqlite3_finalize(stmt) != SQLITE_OK)
			goto error;
	} while (sql);

	lua_pushboolean(T, 1);
	return 1;

error:
	lua_pushnil(T);
	lua_pushstring(T, sqlite3_errmsg(db->handle));
	return 2;
}

static int
db_autocommit(lua_State *T)
{
	struct db *db;

	luaL_checktype(T, 1, LUA_TUSERDATA);
	db = db_unbox(T, 1);
	if (db == NULL) {
		lua_pushnil(T);
		lua_pushliteral(T, "closed");
		return 2;
	}

	lua_pushboolean(T, sqlite3_get_autocommit(db->handle));
	return 1;
}

static int
db_close(lua_State *T)
{
	struct box *box;

	luaL_checktype(T, 1, LUA_TUSERDATA);
	box = lua_touserdata(T, 1);
	if (box->db == NULL) {
		lua_pushnil(T);
		lua_pushliteral(T, "already closed");
		return 2;
	}

	db_free(box->db);
	box->db = NULL;

	lua_pushboolean(T, 1);
	return 1;
}

static int
db_open(lua_State *T)
{
	const char *filename = luaL_checkstring(T, 1);
	int flags = luaL_optnumber(T, 2, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE);
	sqlite3 *handle;
	struct box *box;
	int ret;

	ret = sqlite3_open_v2(filename, &handle, flags, NULL);
	if (handle == NULL)
		return luaL_error(T, "out of memory");

	if (ret != SQLITE_OK) {
		lua_pushnil(T);
		lua_pushstring(T, sqlite3_errmsg(handle));
		(void)sqlite3_close(handle);
		return 2;
	}

	box = lua_newuserdata(T, sizeof(struct box));
	box->db = lem_xmalloc(sizeof(struct db));
	box->db->handle = handle;
	box->db->refs = 1;

	/* set metatable */
	lua_pushvalue(T, lua_upvalueindex(1));
	lua_setmetatable(T, -2);

	return 1;
}

#define set_open_constant(L, name) \
	lua_pushnumber(L, SQLITE_OPEN_##name);\
	lua_setfield(L, -2, #name)

int
luaopen_lem_sqlite3_core(lua_State *L)
{
	/* create module table */
	lua_createtable(L, 0, 11);

	/* create Statement metatable */
	lua_createtable(L, 0, 9);
	lua_pushvalue(L, -1);
	lua_setfield(L, -2, "__index");
	/* insert __gc method */
	lua_pushcfunction(L, stmt_finalize);
	lua_setfield(L, -2, "__gc");
	/* insert finalize method */
	lua_pushcfunction(L, stmt_finalize);
	lua_setfield(L, -2, "finalize");
	/* insert bind method */
	lua_pushcfunction(L, stmt_bind);
	lua_setfield(L, -2, "bind");
	/* insert step method */
	lua_pushcfunction(L, stmt_step);
	lua_setfield(L, -2, "step");
	/* insert reset method */
	lua_pushcfunction(L, stmt_reset);
	lua_setfield(L, -2, "reset");
	/* insert Statement metatable */
	lua_setfield(L, -2, "Statement");

	/* create Connection metatable */
	lua_createtable(L, 0, 9);
	lua_pushvalue(L, -1);
	lua_setfield(L, -2, "__index");
	/* insert __gc method */
	lua_pushcfunction(L, db_close);
	lua_setfield(L, -2, "__gc");
	/* insert close method */
	lua_pushcfunction(L, db_close);
	lua_setfield(L, -2, "close");
	/* insert autocommit method */
	lua_pushcfunction(L, db_autocommit);
	lua_setfield(L, -2, "autocommit");
	/* insert prepare method */
	lua_getfield(L, -2, "Statement"); /* upvalue 1: Statement metatable */
	lua_pushcclosure(L, db_prepare, 1);
	lua_setfield(L, -2, "prepare");
	/* insert exec method */
	lua_pushcfunction(L, db_exec);
	lua_setfield(L, -2, "exec");

	/* insert open function */
	lua_pushvalue(L, -1); /* upvalue 1: Connection metatable */
	lua_pushcclosure(L, db_open, 1);
	lua_setfield(L, -3, "open");

	/* insert Connection metatable */
	lua_setfield(L, -2, "Connection");

	set_open_constant(L, NOMUTEX);
	set_open_constant(L, FULLMUTEX);
	set_open_constant(L, SHAREDCACHE);
	set_open_constant(L, PRIVATECACHE);
	set_open_constant(L, URI);

	set_open_constant(L, READONLY);
	set_open_constant(L, READWRITE);
	set_open_constant(L, CREATE);

	return 1;
}
