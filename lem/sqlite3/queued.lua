--
-- This file is part of lem-sqlite3.
-- Copyright 2011-2012 Emil Renner Berthing
--
-- lem-sqlite3 is free software: you can redistribute it and/or
-- modify it under the terms of the GNU General Public License as
-- published by the Free Software Foundation, either version 3 of
-- the License, or (at your option) any later version.
--
-- lem-sqlite3 is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU General Public License for more details.
--
-- You should have received a copy of the GNU General Public License
-- along with lem-sqlite3.  If not, see <http://www.gnu.org/licenses/>.
--

local utils   = require 'lem.utils'
local sqlite3 = require 'lem.sqlite3'

local setmetatable = setmetatable
local thisthread, suspend, resume
	= utils.thisthread, utils.suspend, utils.resume

local Connection = {}
Connection.__index = Connection

function Connection:close(...)
	return self.db:close(...)
end

local Statement = {}
Statement.__index = Statement

do
	local function get(queue)
		local nxt = queue.tail
		if nxt == 0 then
			nxt = 1
			queue.tail = 1
		else
			local me = nxt

			queue[me] = thisthread()
			nxt = #queue+1
			queue.tail = nxt
			suspend()
			queue[me] = nil
		end

		queue.next = nxt
		return queue.db
	end
	Connection.get = get

	local function put(queue, ...)
		local T = queue[queue.next]
		if T then
			resume(T)
		else
			queue.tail = 0
		end
		return ...
	end
	Connection.put = put

	function Connection:prepare(...)
		local stmt, err = put(self, get(self):prepare(...))
		if not stmt then return nil, err end
		return setmetatable({ stmt = stmt, queue = self }, Statement)
	end

	local function db_wrap(method)
		return function(queue, ...)
			return put(queue, method(get(queue), ...))
		end
	end

	Connection.last_insert_rowid  = db_wrap(sqlite3.Connection.last_insert_rowid)
	Connection.changes            = db_wrap(sqlite3.Connection.changes)
	Connection.autocommit         = db_wrap(sqlite3.Connection.autocommit)
	Connection.exec               = db_wrap(sqlite3.Connection.exec)

	Connection.fetchall           = sqlite3.Connection.fetchall
	Connection.fetchone           = sqlite3.Connection.fetchone
	Connection.rows               = sqlite3.Connection.rows

	function Statement:get()
		get(self.queue)
		return self.stmt
	end

	function Statement:put(...)
		return put(self.queue, ...)
	end

	local function stmt_wrap(method)
		return function(self, ...)
			local queue = self.queue
			get(queue)
			return put(queue, method(self.stmt, ...))
		end
	end

	local Statement = sqlite3.Statement
	Statement.finalize = stmt_wrap(Statement.finalize)
	Statement.fetchall = stmt_wrap(Statement.fetchall)
	Statement.fetchone = stmt_wrap(Statement.fetchone)
	Statement.rows     = stmt_wrap(Statement.rows)
end

local open
do
	local uopen = sqlite3.open

	function open(...)
		local db, err = uopen(...)
		if not db then return nil, err end

		return setmetatable({ db = db, tail = 0, next = 0 }, Connection)
	end
end

return {
	Connection   = Connection,
	Statement    = Statement,
	open         = open,

	NOMUTEX      = sqlite3.NOMUTEX,
	FULLMUTEX    = sqlite3.FULLMUTEX,
	SHAREDCACHE  = sqlite3.SHAREDCACHE,
	PRIVATECACHE = sqlite3.PRIVATECACHE,
	URI          = sqlite3.URI,

	READONLY     = sqlite3.READONLY,
	READWRITE    = sqlite3.READWRITE,
	CREATE       = sqlite3.CREATE,
}

-- vim: set ts=2 sw=2 noet:
