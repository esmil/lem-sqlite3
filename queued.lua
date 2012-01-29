--
-- This file is part of lem-streams.
-- Copyright 2011 Emil Renner Berthing
--
-- lem-streams is free software: you can redistribute it and/or
-- modify it under the terms of the GNU General Public License as
-- published by the Free Software Foundation, either version 3 of
-- the License, or (at your option) any later version.
--
-- lem-streams is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU General Public License for more details.
--
-- You should have received a copy of the GNU General Public License
-- along with lem-streams.  If not, see <http://www.gnu.org/licenses/>.
--

local utils   = require 'lem.utils'
local sqlite3 = require 'lem.sqlite3'

local setmetatable = setmetatable
local thisthread, suspend, resume
	= utils.thisthread, utils.suspend, utils.resume

local QConnection = {}
QConnection.__index = QConnection

function QConnection:close(...)
	return self.db:close(...)
end

local QStatement = {}
QStatement.__index = QStatement

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
	QConnection.get = get

	local function put(queue, ...)
		local T = queue[queue.next]
		if T then
			resume(T)
		else
			queue.tail = 0
		end
		return ...
	end
	QConnection.put = put

	function QConnection:prepare(...)
		local stmt, err = put(self, get(self):prepare(...))
		if not stmt then return nil, err end
		return setmetatable({ stmt = stmt, queue = self }, QStatement)
	end

	local function db_wrap(method)
		return function(queue, ...)
			return put(queue, method(get(queue), ...))
		end
	end

	local Connection = sqlite3.Connection
	QConnection.exec     = db_wrap(Connection.exec)

	QConnection.fetchall = Connection.fetchall
	QConnection.fetchone = Connection.fetchone
	QConnection.rows     = Connection.rows

	function QStatement:get()
		get(self.queue)
		return self.stmt
	end

	function QStatement:put(...)
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
	QStatement.finalize = stmt_wrap(Statement.finalize)
	QStatement.fetchall = stmt_wrap(Statement.fetchall)
	QStatement.fetchone = stmt_wrap(Statement.fetchone)
	QStatement.rows     = stmt_wrap(Statement.rows)
end

return {
	QConnection = QConnection,
	QStatement  = QStatement,
	open = function(...)
		local db, err = sqlite3.open(...)
		if not db then return nil, err end

		return setmetatable({ db = db, tail = 0, next = 0 }, QConnection)
	end,

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
