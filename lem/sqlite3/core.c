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
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <lem.h>
#include <pthread.h>
#include <sqlite3.h>

enum req {
	REQ_NONE,
	REQ_PREP,
	REQ_STEP,
};

struct req_open {
	enum req type;
	int flags;
	const char *filename;
};

struct req_prep {
	enum req type;
	int len;
	const char *sql;
	sqlite3_stmt *stmt;
	const char *tail;
};

struct db {
	ev_async w;
	sqlite3 *handle;
	unsigned int refs;
	int pipe;
	int ret;
	pthread_t thread;
	union {
		enum req type;
		struct req_open open;
		struct req_prep prep;
	} req;
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
db_worker_notify(struct db *db)
{
	char c;
	(void)write(db->pipe, &c, 1);
}

static void *
db_worker(void *data)
{
	struct db *db = data;
	int fd = db->ret;
	char c;

	lem_debug("OPEN");
	db->ret = sqlite3_open_v2(db->req.open.filename,
			&db->handle, db->req.open.flags, NULL);
	ev_async_send(LEM_ &db->w);

	while (read(fd, &c, 1) > 0) {
		switch (db->req.type) {
		case REQ_NONE:
			lem_debug("REQ_NONE");
			continue;

		case REQ_PREP:
			lem_debug("REQ_PREP");
			db->ret = sqlite3_prepare_v2(db->handle,
					db->req.prep.sql,
					db->req.prep.len,
					&db->req.prep.stmt,
					&db->req.prep.tail);
			break;

		case REQ_STEP:
			lem_debug("REQ_STEP");
			db->ret = sqlite3_step(db->req.prep.stmt);
			break;
		}

		db->req.type = REQ_NONE;
		ev_async_send(LEM_ &db->w);
	}

	lem_debug("EXIT!");
	close(fd);
	return NULL;
}

static void
db_unref(struct db *db)
{
	db->refs--;
	if (db->refs)
		return;

	lem_debug("db->refs = %d, freeing", db->refs);

	pthread_detach(db->thread);
	close(db->pipe);
	(void)sqlite3_close(db->handle);
	free(db);
}

static int
stmt_finalize(lua_State *T)
{
	struct stmt *stmt;
	struct db *db;
	int ret;

	luaL_checktype(T, 1, LUA_TUSERDATA);
	stmt = lua_touserdata(T, 1);
	if (stmt->handle == NULL) {
		lua_pushnil(T);
		lua_pushliteral(T, "already finalized");
		return 2;
	}

	db = stmt->db;
	if (db->w.data != NULL) {
		lua_pushnil(T);
		lua_pushliteral(T, "busy");
		return 2;
	}

	if (sqlite3_finalize(stmt->handle) == SQLITE_OK) {
		lua_pushboolean(T, 1);
		ret = 1;
	} else {
		lua_pushnil(T);
		lua_pushstring(T, sqlite3_errmsg(db->handle));
		ret = 2;
	}

	stmt->handle = NULL;
	db_unref(db);
	return ret;
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

	if (stmt->db->w.data != NULL) {
		lua_pushnil(T);
		lua_pushliteral(T, "busy");
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
stmt_column_names(lua_State *T)
{
	struct stmt *stmt;
	int columns;
	int i;

	luaL_checktype(T, 1, LUA_TUSERDATA);
	stmt = lua_touserdata(T, 1);
	if (stmt->handle == NULL) {
		lua_pushnil(T);
		lua_pushliteral(T, "finalized");
		return 2;
	}

	if (stmt->db->w.data != NULL) {
		lua_pushnil(T);
		lua_pushliteral(T, "busy");
		return 2;
	}

	columns = sqlite3_column_count(stmt->handle);
	lua_createtable(T, columns, 0);
	i = 0;
	while (i < columns) {
		const char *name = sqlite3_column_name(stmt->handle, i++);

		if (name == NULL)
			return luaL_error(T, "out of memory");

		lua_pushstring(T, name);
		lua_rawseti(T, -2, i);
	}

	return 1;
}

static void
stmt_step_handler(EV_P_ ev_async *w, int revents)
{
	struct db *db = (struct db *)w;
	lua_State *T = db->w.data;
	sqlite3_stmt *stmt = db->req.prep.stmt;
	int ret = 1;

	(void)revents;

	ev_async_stop(EV_A_ &db->w);
	db->w.data = NULL;

	switch (db->ret) {
	case SQLITE_ROW:
		lem_debug("SQLITE_ROW");
		pushrow(T, stmt);
		break;

	case SQLITE_DONE:
		lem_debug("SQLITE_DONE");
		(void)sqlite3_reset(stmt);
		(void)sqlite3_clear_bindings(stmt);
		lua_pushboolean(T, 1);
		break;

	default:
		lem_debug("Ack!");
		lua_pushnil(T);
		lua_pushstring(T, sqlite3_errmsg(db->handle));
		ret = 2;
	}

	lem_queue(T, ret);
}

static int
stmt_step(lua_State *T)
{
	struct stmt *stmt;
	struct db *db;

	luaL_checktype(T, 1, LUA_TUSERDATA);
	stmt = lua_touserdata(T, 1);
	if (stmt->handle == NULL) {
		lua_pushnil(T);
		lua_pushliteral(T, "finalized");
		return 2;
	}

	db = stmt->db;
	if (db->w.data != NULL) {
		lua_pushnil(T);
		lua_pushliteral(T, "busy");
		return 2;
	}

	db->w.cb = stmt_step_handler;
	db->w.data = T;
	ev_async_start(LEM_ &db->w);
	db->req.type = REQ_STEP;
	db->req.prep.stmt = stmt->handle;
	db_worker_notify(db);

	lua_settop(T, 1);
	return lua_yield(T, 1);
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

	if (stmt->db->w.data != NULL) {
		lua_pushnil(T);
		lua_pushliteral(T, "busy");
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

static void
db_prepare_handler(EV_P_ ev_async *w, int revents)
{
	struct db *db = (struct db *)w;
	lua_State *T = db->w.data;
	int ret = 1;

	(void)revents;

	ev_async_stop(EV_A_ &db->w);
	db->w.data = NULL;

	if (db->ret != SQLITE_OK) {
		lem_debug("db->ret != SQLITE_OK");
		lua_pushnil(T);
		lua_pushstring(T, sqlite3_errmsg(db->handle));
		ret = 2;
		db_unref(db);
	} else if (db->req.prep.stmt == NULL) {
		lem_debug("db->req.prep.stmt == NULL");
		lua_pushnil(T);
		lua_pushliteral(T, "nosql");
		ret = 2;
		db_unref(db);
	} else {
		struct stmt *stmt = lua_newuserdata(T, sizeof(struct stmt));

		lem_debug("Success!");
		stmt->handle = db->req.prep.stmt;
		stmt->db = db;

		/* set metatable */
		lua_pushvalue(T, 3);
		lua_setmetatable(T, -2);
	}

	lem_queue(T, ret);
}

static int
db_prepare(lua_State *T)
{
	struct db *db;
	const char *sql;
	size_t len;

	luaL_checktype(T, 1, LUA_TUSERDATA);
	sql = luaL_checklstring(T, 2, &len);

	db = db_unbox(T, 1);
	if (db == NULL) {
		lua_pushnil(T);
		lua_pushliteral(T, "closed");
		return 2;
	}

	if (db->w.data != NULL) {
		lua_pushnil(T);
		lua_pushliteral(T, "busy");
		return 2;
	}

	db->refs++;
	db->w.cb = db_prepare_handler;
	db->w.data = T;
	ev_async_start(LEM_ &db->w);
	db->req.type = REQ_PREP;
	db->req.prep.sql = sql;
	db->req.prep.len = len+1;
	db_worker_notify(db);

	lua_settop(T, 2);
	lua_pushvalue(T, lua_upvalueindex(1));
	return lua_yield(T, 3);
}

static void db_exec_step_handler(EV_P_ ev_async *w, int revents);

static void
db_exec_prep_handler(EV_P_ ev_async *w, int revents)
{
	struct db *db = (struct db *)w;
	lua_State *T = db->w.data;
	int ret = 1;

	(void)revents;

	if (db->ret != SQLITE_OK) {
		lua_pushnil(T);
		lua_pushstring(T, sqlite3_errmsg(db->handle));
		ret = 2;
		goto out;
	}

	if (db->req.prep.stmt == NULL) {
		lua_pushboolean(T, 1);
		goto out;
	}

	if (lua_gettop(T) > 1 && bindtable(T, db->req.prep.stmt)) {
		(void)sqlite3_finalize(db->req.prep.stmt);
		lua_pushnil(T);
		lua_insert(T, -2);
		ret = 2;
		goto out;
	}

	db->w.cb = db_exec_step_handler;
	db->req.type = REQ_STEP;
	db_worker_notify(db);
	return;

out:
	ev_async_stop(EV_A_ &db->w);
	db->w.data = NULL;
	lem_queue(T, ret);
}

static void
db_exec_step_handler(EV_P_ ev_async *w, int revents)
{
	struct db *db = (struct db *)w;
	lua_State *T = db->w.data;
	int ret = 1;

	(void)revents;

	switch (db->ret) {
	case SQLITE_ROW:
	case SQLITE_DONE:
		break;

	default:
		lua_pushnil(T);
		lua_pushstring(T, sqlite3_errmsg(db->handle));
		(void)sqlite3_finalize(db->req.prep.stmt);
		ret = 2;
		goto out;
	}

	if (sqlite3_finalize(db->req.prep.stmt) != SQLITE_OK) {
		lua_pushnil(T);
		lua_pushstring(T, sqlite3_errmsg(db->handle));
		ret = 2;
		goto out;
	}

	if (db->req.prep.tail == NULL) {
		lua_pushboolean(T, 1);
		goto out;
	}

	db->w.cb = db_exec_prep_handler;
	db->req.type = REQ_PREP;
	db->req.prep.len -= db->req.prep.tail - db->req.prep.sql;
	db->req.prep.sql = db->req.prep.tail;
	db_worker_notify(db);
	return;

out:
	ev_async_stop(EV_A_ &db->w);
	db->w.data = NULL;
	lem_queue(T, ret);
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

	if (db->w.data != NULL) {
		lua_pushnil(T);
		lua_pushliteral(T, "busy");
		return 2;
	}

	db->w.cb = db_exec_prep_handler;
	db->w.data = T;
	ev_async_start(LEM_ &db->w);
	db->req.type = REQ_PREP;
	db->req.prep.sql = sql;
	db->req.prep.len = len+1;
	db_worker_notify(db);

	return lua_yield(T, bind ? 2 : 1);
}

static int
db_last_insert_rowid(lua_State *T)
{
	struct db *db;

	luaL_checktype(T, 1, LUA_TUSERDATA);
	db = db_unbox(T, 1);
	if (db == NULL) {
		lua_pushnil(T);
		lua_pushliteral(T, "closed");
		return 2;
	}

	if (db->w.data != NULL) {
		lua_pushnil(T);
		lua_pushliteral(T, "busy");
		return 2;
	}

	lua_pushnumber(T, sqlite3_last_insert_rowid(db->handle));
	return 1;
}

static int
db_changes(lua_State *T)
{
	struct db *db;

	luaL_checktype(T, 1, LUA_TUSERDATA);
	db = db_unbox(T, 1);
	if (db == NULL) {
		lua_pushnil(T);
		lua_pushliteral(T, "closed");
		return 2;
	}

	if (db->w.data != NULL) {
		lua_pushnil(T);
		lua_pushliteral(T, "busy");
		return 2;
	}

	lua_pushnumber(T, sqlite3_changes(db->handle));
	return 1;
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

	if (db->w.data != NULL) {
		lua_pushnil(T);
		lua_pushliteral(T, "busy");
		return 2;
	}

	lua_pushboolean(T, sqlite3_get_autocommit(db->handle));
	return 1;
}

static int
db_close(lua_State *T)
{
	struct box *box;
	struct db *db;

	luaL_checktype(T, 1, LUA_TUSERDATA);
	box = lua_touserdata(T, 1);
	db = box->db;
	if (db == NULL) {
		lua_pushnil(T);
		lua_pushliteral(T, "already closed");
		return 2;
	}

	if (db->w.data != NULL) {
		lua_State *S = db->w.data;

		ev_async_stop(LEM_ &db->w);
		db->w.data = NULL;

		lua_settop(S, 0);
		lua_pushnil(S);
		lua_pushliteral(S, "interrupted");
		lem_queue(S, 2);
	}

	db_unref(db);
	box->db = NULL;

	lua_pushboolean(T, 1);
	return 1;
}

static void
db_open_handler(EV_P_ ev_async *w, int revents)
{
	struct db *db = (struct db *)w;
	lua_State *T = db->w.data;
	int ret = 1;

	(void)revents;

	ev_async_stop(EV_A_ &db->w);
	db->w.data = NULL;

	if (db->handle == NULL) {
		lem_debug("db->handle == NULL");
		lua_pushnil(T);
		lua_pushliteral(T, "out of memory");
		db_unref(db);
		ret = 2;
	} else if (db->ret != SQLITE_OK) {
		lem_debug("db->ret != SQLITE_OK");
		lua_pushnil(T);
		lua_pushstring(T, sqlite3_errmsg(db->handle));
		db_unref(db);
		ret = 2;
	} else {
		struct box *box = lua_newuserdata(T, sizeof(struct box));

		lem_debug("Success!");
		box->db = db;

		/* set metatable */
		lua_pushvalue(T, 2);
		lua_setmetatable(T, -2);
	}

	lem_queue(T, ret);
}

static int
db_open(lua_State *T)
{
	const char *filename = luaL_checkstring(T, 1);
	int flags = luaL_optnumber(T, 2, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE);
	struct db *db;
	int fds[2];

	if (pipe(fds)) {
		lua_pushnil(T);
		lua_pushfstring(T, "error creating pipe: %s",
				strerror(errno));
		return 2;
	}

	db = lem_xmalloc(sizeof(struct db));
	ev_async_init(&db->w, db_open_handler);
	ev_async_start(LEM_ &db->w);
	db->w.data = T;
	db->handle = NULL;
	db->refs = 1;
	db->ret = fds[0];
	db->pipe = fds[1];
	db->req.open.filename = filename;
	db->req.open.flags = flags;
	if (pthread_create(&db->thread, NULL, db_worker, db)) {
		ev_async_stop(LEM_ &db->w);
		close(fds[0]);
		close(fds[1]);
		free(db);
		lua_pushnil(T);
		lua_pushliteral(T, "error creating worker thread");
		return 2;
	}

	lua_settop(T, 1);
	lua_pushvalue(T, lua_upvalueindex(1));
	return lua_yield(T, 2);
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
	lua_createtable(L, 0, 10);
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
	/* insert column_names method */
	lua_pushcfunction(L, stmt_column_names);
	lua_setfield(L, -2, "column_names");
	/* insert step method */
	lua_pushcfunction(L, stmt_step);
	lua_setfield(L, -2, "step");
	/* insert reset method */
	lua_pushcfunction(L, stmt_reset);
	lua_setfield(L, -2, "reset");
	/* insert Statement metatable */
	lua_setfield(L, -2, "Statement");

	/* create Connection metatable */
	lua_createtable(L, 0, 10);
	lua_pushvalue(L, -1);
	lua_setfield(L, -2, "__index");
	/* insert __gc method */
	lua_pushcfunction(L, db_close);
	lua_setfield(L, -2, "__gc");
	/* insert close method */
	lua_pushcfunction(L, db_close);
	lua_setfield(L, -2, "close");
	/* insert last_insert_rowid method */
	lua_pushcfunction(L, db_last_insert_rowid);
	lua_setfield(L, -2, "last_insert_rowid");
	/* insert changes method */
	lua_pushcfunction(L, db_changes);
	lua_setfield(L, -2, "changes");
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
