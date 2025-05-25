--!strict

--Hoopa is a custom DataStore Wrapper for Roblox
--The name Hoopa is inspired by the Pokemon Hoopa, which is known for his mischief!
--This wrapper is propietary, use without permission from Indicy is not allowed!
--If you want to use this wrapper, DM me on Discord: @indicy
local RunService = game:GetService("RunService")
local MemoryStoreService = game:GetService("MemoryStoreService")
local DataStoreService = game:GetService("DataStoreService")

local module = {}

--//Private Variables
local RETRY_LIMIT = 5 -- The maximum number of retries for a request
local RETRY_DELAY = 10 -- The delay between retries in seconds
local LAST_SAVE_TIMEOUT = 120 -- The time for a session to be deemed inactive
local CRITICAL_TIMEOUT = 30 -- The time for the server to shut down in critical state

local AUTO_UPDATE = 60 -- The time between auto updates in seconds

local FAILURE_COUNT = 0 -- The number of failed requests
local MAX_FAILURE_COUNT = 5 -- The maximum number of failed requests before the server is deemed to be in critical state

local INITIALIZED = false -- The state of the module, if true, the module is initialized
local CRITICAL_STATE = false -- The state of the server, if true, new requests will be rejected.
local SERVER_CLOSING = false -- The state of the server, if true, the server is closing.

local LockMap = MemoryStoreService:GetSortedMap("Hoopa-Lock") -- Memory map for session locking

local TOKEN = game.JobId

--// Private Types
type ErrorLog = {
	Time: number,
	ErrorType: string,
	ErrorMessage: string,
}

type Profile<T> = {
	["Data"]: T,
	["LAST_SAVE"]: number,
	["ErrorLog"]: { ErrorLog },
}

type Session<T> = {
	["Data"]: T,
	["LAST_SAVE"]: number,
	["IsActive"]: boolean,
	["IsLoaded"]: boolean,
	["IsSaving"]: boolean,
	["Retries"]: number,
	["ErrorLog"]: { ErrorLog },
}

--// Public Types
export type Hoopa<T> = typeof(setmetatable(
	{} :: {
		["ActiveSessions"]: { [Player]: Session<T> },
		["DataStore"]: DataStore,
		["DefaultData"]: T,
	},
	module
))

--// Private functions
local function ConstructProfile<T>(session: Session<T>): Profile<T>
	local profile = {
		["Data"] = session.Data,
		["LAST_SAVE"] = os.time(),
		["ErrorLog"] = session.ErrorLog,
	}
	return profile
end

--// Private methods

--// Creates a deep copy of a table
local function deepCopy<T>(tbl: T): T
	local function _deepCopy(value: any): any
		if type(value) == "table" then
			local result = {}
			for k, v in pairs(value) do
				result[_deepCopy(k)] = _deepCopy(v)
			end
			return result
		else
			return value
		end
	end

	return _deepCopy(tbl) :: T
end

--// Public methods

--//Ends the session for a player
function module.EndSession<T>(self: Hoopa<T>, Player: Player)
	local session = self.ActiveSessions[Player]
	if not session then
		warn("Session not found for player: " .. Player.Name)
		return
	end

	session.IsActive = false
	self:UpdateAsync(Player)

	--// Remove the player's session token from the lock map
	LockMap:RemoveAsync(tostring(Player.UserId))
end

--// Creates new profile for the player
function module.CreateProfile<T>(self: Hoopa<T>): Profile<T>
	local profile: Profile<T> = {
		["Data"] = deepCopy(self.DefaultData),
		["LAST_SAVE"] = 0,
		["ErrorLog"] = {},
	}
	return profile
end

--// Updates the datastore with the current profile
function module.UpdateAsync<T>(self: Hoopa<T>, Player: Player): boolean
	local session = self.ActiveSessions[Player]
	if not session then
		warn("Session not found for player: " .. Player.Name)
		return false
	end

	session.IsSaving = true

	local profile = ConstructProfile(session)

	--// Update the profile in the DataStore
	local success, result = pcall(function()
		self.DataStore:SetAsync(tostring(Player.UserId), profile)
	end)

	--// Update the session token in lock map
	LockMap:SetAsync(tostring(Player.UserId), TOKEN, LAST_SAVE_TIMEOUT)

	session.IsSaving = false

	if not success then
		warn("Failed to update profile for player: " .. Player.Name .. " - " .. result)
		FAILURE_COUNT += 1
		return false
	end

	FAILURE_COUNT = 0

	session.LAST_SAVE = os.time()
	return true
end

--// Gets player profile and checks to ensure it's not active in another server
function module.GetAsync<T>(self: Hoopa<T>, Player: Player): Profile<T>?
	--// Check for active session token
	local currentToken = LockMap:GetAsync(tostring(Player.UserId))
	if currentToken then
		Player:Kick("Active Session Detected! Please rejoin the game.")
		return nil
	end

	local profile = nil

	--// Get data from DataStore
	for _ = 1, RETRY_LIMIT do
		local success: boolean, result: any = pcall(function()
			local Data = self.DataStore:GetAsync(tostring(Player.UserId))
			return Data
		end)

		if success then
			profile = result
			break
		end

		task.wait(RETRY_DELAY)
	end

	--// If no data is found, create a new profile
	if profile == nil then
		profile = self.CreateProfile(self)
	end

	--// Function to reconcile the profile with default data
	local function Reconcile()
		local defaultData = self.DefaultData
		local oldData = profile.Data :: any

		for i, v in defaultData :: any do
			if oldData[i] ~= nil then
				continue
			end
			oldData[i] = v
		end

		profile.Data = oldData
	end

	Reconcile()

	return profile
end

--// Create a new session for player
function module.NewSession<T>(self: Hoopa<T>, Player: Player): Session<T>?
	local profile = self:GetAsync(Player)

	if profile == nil then
		return nil
	end

	--// Check for missing data and add default values
	local function Reconcile(Session: Session<T>)
		local defaultData = self.DefaultData
		local oldData = Session.Data :: any

		for i, v in defaultData :: any do
			if oldData[i] ~= nil then
				continue
			end
			oldData[i] = v
		end

		Session.Data = oldData
	end

	local session = {
		["Data"] = profile.Data,
		["LAST_SAVE"] = profile.LAST_SAVE,
		["IsActive"] = true,
		["IsLoaded"] = false,
		["IsSaving"] = false,
		["Retries"] = 0,
		["ErrorLog"] = {},
	}

	Reconcile(session)

	self.ActiveSessions[Player] = session
	session.IsLoaded = true

	return session
end

--// Gets player data from session
function module.GetPlayerData<T>(self: Hoopa<T>, Player: Player): T?
	local session = self.ActiveSessions[Player]
	if session then
		return session.Data
	else
		return nil
	end
end

function module.SetPlayerData<T>(self: Hoopa<T>, Player: Player, Data: T): ()
	local session = self.ActiveSessions[Player]
	if not session then
		warn("Session not found for player: " .. Player.Name)
		return
	end

	session.Data = Data
end

--// Protocol to follow if server is in critical state
function module.CRITICAL_PROTOCOL<T>(self: Hoopa<T>)
	if RunService:IsStudio() then
		return
	end

	CRITICAL_STATE = true

	--// Countdown for server shutdown
	coroutine.wrap(function()
		local timeLeft = CRITICAL_TIMEOUT

		repeat
			warn(("Server is in critical state! Server closing in %i"):format(timeLeft))
			task.wait(1)
			timeLeft -= 1
		until timeLeft <= 0 or not CRITICAL_STATE

		if CRITICAL_STATE then
			game:Shutdown()
		end
	end)()

	--// Kick all players from the server
	for player, session in self.ActiveSessions do
		session.IsActive = false
		player:Kick(
			"Server is in critical state! You have been kicked to protect your data. If this persists upon rejoin, please try again later."
		)
	end
end

--// method to run when player is added
function module.PlayerAdded<T>(self: Hoopa<T>, Player: Player): ()
	if CRITICAL_STATE then
		Player:Kick("Server is in critical state! Please join another server.")
		return
	end

	if self.ActiveSessions[Player] then
		self:EndSession(Player)
		Player:Kick("Active Session Detected! Please rejoin the game.")
		return nil
	end

	local session = self:NewSession(Player)

	if session == nil then
		return
	end

	--// On a loop update data, if server is closing or critical state then break loop
	coroutine.wrap(function()
		repeat
			task.wait(AUTO_UPDATE)
			if session.IsSaving then
				continue
			elseif CRITICAL_STATE then
				continue
			elseif SERVER_CLOSING then
				break
			end

			self:UpdateAsync(Player)

		until not session.IsActive or CRITICAL_STATE or SERVER_CLOSING
	end)()
end

--// method to tun when player is removed
function module.PlayerRemoving<T>(self: Hoopa<T>, Player: Player): ()
	if CRITICAL_STATE then
		return
	end

	if self.ActiveSessions[Player] then
		self:EndSession(Player)
	end
end

--// function to run when server is shutting down
function module.BindToClose<T>(self: Hoopa<T>): ()
	SERVER_CLOSING = true

	if CRITICAL_STATE then
		return
	end

	for player in self.ActiveSessions do
		self:EndSession(player)
		player:Kick("This server has shut down.")
	end
end

--// Initializes the module and returns the Hoopa Instance
function Init<T>(DataStoreName: string, DefaultData: T): Hoopa<T>?
	if INITIALIZED then
		warn("Only one instance of Hoopa can be created! Please use the existing instance.")
		return nil
	end

	local loadedPlayers = {} :: { [Player]: Session<T> }

	local function getDataStore(): DataStore
		local dataStore = DataStoreService:GetDataStore(DataStoreName)
		return dataStore
	end

	local DataStore = getDataStore()

	local Hoopa = {
		ActiveSessions = loadedPlayers,
		DataStore = DataStore,
		DefaultData = DefaultData,
	}

	setmetatable(Hoopa, module)

	local heartbeat = nil

	heartbeat = RunService.Heartbeat:Connect(function()
		if FAILURE_COUNT >= MAX_FAILURE_COUNT then
			Hoopa:CRITICAL_PROTOCOL()
			heartbeat:Disconnect()
		end
	end)

	game:BindToClose(function()
		Hoopa:BindToClose()
	end)

	return Hoopa
end

module.__index = module

return { Init = Init }
