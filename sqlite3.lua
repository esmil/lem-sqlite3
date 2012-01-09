--
-- This file is part of lem-sqlite3.
-- Copyright 2012 Emil Renner Berthing
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

local M = require 'lem.sqlite3.core'
local select, assert, type = select, assert, type
local Statement = M.Statement
local Connection = M.Connection

function Statement:fetchall(...)
	if select('#', ...) > 0 then self:bind(...) end

	local t, n = {}, 0
	while true do
		local row, err = self:step()
		if not row then return nil, err end
		if row == true then break end

		n = n + 1
		t[n] = row
	end

	return t
end

function Statement:fetchone(...)
	if select('#', ...) > 0 then self:bind(...) end

	local row, err = self:step()
	if not row then return nil, err end
	if row == true then	return true end

	local ok, err = self:reset()
	if not ok then return nil, err end

	return row
end

function Statement:rows(...)
	if select('#', ...) > 0 then self:bind(...) end

	return function(stmt)
		local row = assert(stmt:step())
		if row == true then return nil end
		return row
	end, self
end

function Connection:fetchall(sql, ...)
	local stmt, err = self:prepare(sql)
	if not stmt then return nil, err end

	local r, err = stmt:fetchall(...)
	if not r then
		stmt:finalize()
		return nil, err
	end

	local ok, err = stmt:finalize()
	if not ok then return nil, err end

	return r
end

function Connection:fetchone(sql, ...)
	local stmt, err = self:prepare(sql)
	if not stmt then return nil, err end

	local r, err = stmt:fetchone(...)
	if not r then
		stmt:finalize()
		return nil, err
	end

	local ok, err = stmt:finalize()
	if not ok then return nil, err end

	return r
end

function Connection:rows(sql, ...)
	local stmt = assert(self:prepare(sql))
	if select('#', ...) > 0 then stmt:bind(...) end

	return function(stmt)
		local row = assert(stmt:step())
		if row == true then
			assert(stmt:finalize())
			return nil
		end
		return row
	end, stmt
end

return M

-- vim: set ts=2 sw=2 noet:
