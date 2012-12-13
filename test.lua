#!/usr/bin/env lem
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

package.path = '?.lua;' .. package.path
package.cpath = '?.so;' .. package.cpath

local format = string.format
local write = io.write
local tostring = tostring

local function printf(...)
	return write(format(...))
end

local function prettyprint(t)
	local widths, columns = {}, #t[1]
	for i = 1, columns do
		widths[i] = 0
	end

	for i = 1, #t do
		local row = t[i]
		for j = 1, columns do
			local value = row[j]
			local typ = type(value)
			if typ == 'nil' then
				value = 'NULL'
			elseif typ == 'string' then
				value = format("'%s'", value)
			else
				value = tostring(value)
			end

			local len = #value
			if len > widths[j] then widths[j] = #value end
			row[j] = value
		end
	end

	for i = 1, #widths do
		widths[i] = '%-' .. tostring(widths[i] + 1) .. 's';
	end

	for i = 1, #t do
		local row = t[i]
		for j = 1, columns do
			write(format(widths[j], row[j] or 'NULL'))
		end
		write('\n')
	end
end

local utils  = require 'lem.utils'
local io     = require 'lem.io'
--local sqlite = require 'lem.sqlite3'
local sqlite = require 'lem.sqlite3.queued'

local exit = false
utils.spawn(function()
	local write, yield = io.write, utils.yield
	local sleeper = utils.newsleeper()

	repeat
		write('.')
		--yield()
		sleeper:sleep(0.001)
	until exit
end)

local db, err = sqlite.open('test.db', sqlite.READWRITE)

if not db then
	if err ~= 'unable to open database file' then error(err) end

	db = assert(sqlite.open('test.db', sqlite.READWRITE + sqlite.CREATE))

	assert(db:exec[[
CREATE TABLE accounts (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	hash TEXT UNIQUE,
	member TEXT,
	balance FLOAT
);
CREATE TABLE products (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	barcode TEXT UNIQUE,
	price FLOAT,
	name TEXT
);
CREATE TABLE purchases (
	dt TEXT,
	product_id INTEGER,
	account_id INTEGER,
	amount FLOAT
);
]])

---[[
	local stmt = assert(db:prepare('\z
		INSERT INTO accounts (hash, member, balance) \z
		VALUES (@hash, @member, @balance)'))
	local queued = type(stmt) ~= 'userdata'

	for _, v in ipairs{
		{ hash = 'ABC', member = 'Esmil',   balance = 7 },
		{ hash = 'XYZ', member = 'SiGNOUT', balance = -1000 },
--[=[
--]]
	local stmt = assert(db:prepare('\z
		INSERT INTO accounts (hash, member, balance) \z
		VALUES (?, ?, ?)'))
	for _, v in ipairs{
		{ 'ABC', 'Esmil',    7 },
		{ 'XYZ', 'SiGNOUT', -1000 },
--]=]
	} do
		local raw = stmt
		if queued then raw = stmt:get() end

		raw:bind(v)
		assert(assert(raw:step()) == true, 'hmm..')

		if queued then stmt:put() end
	end

	assert(stmt:finalize())
end

local res = assert(db:fetchall('SELECT * FROM accounts'))
print()
prettyprint(res)

assert(db:close())

print()
printf("sqlite.READONLY  = %d\n", sqlite.READONLY)
printf("sqlite.READWRITE = %d\n", sqlite.READWRITE)
printf("sqlite.CREATE    = %d\n", sqlite.CREATE)

exit = true

-- vim: set ts=2 sw=2 noet:
