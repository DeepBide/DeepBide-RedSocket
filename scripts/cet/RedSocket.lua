-- MIT: original RedSocket by Louis Poirier; modifications by DeepBide.
---@class RedSocket
---@field socket any?
---@field proxy any?
local RedSocket = {}

function RedSocket:new(onDestroy)
  local native = Socket.Create()
  if native == nil then return nil end
  local obj = setmetatable({}, { __index = RedSocket })
  obj.socket = native
  obj.proxy = nil
  obj.onDestroy = onDestroy
  return obj
end

function RedSocket:IsRetired()
  return self.socket == nil or self.socket:IsRetired()
end

function RedSocket:IsConnected()
  return not self:IsRetired() and self.socket:IsConnected()
end

---@param onCommand function callback for a received line
---@param onConnection function callback for a connection result
---@param onDisconnection function? optional disconnection callback
---@param onError function? optional send-error callback
function RedSocket:RegisterListener(onCommand, onConnection, onDisconnection, onError)
  local native = self.socket
  if self:IsRetired() or type(onCommand) ~= "function" or type(onConnection) ~= "function"
      or (onDisconnection ~= nil and type(onDisconnection) ~= "function")
      or (onError ~= nil and type(onError) ~= "function") then
    return false
  end
  local proxy
  proxy = NewProxy({
    ["OnCommand"] = {
      args = {"String"},
      callback = function(command)
        if self.socket == native and self.proxy == proxy then onCommand(command) end
      end,
    },
    ["OnConnection"] = {
      args = {"Int32"},
      callback = function(status)
        if self.socket == native and self.proxy == proxy then onConnection(status) end
      end,
    },
    ["OnDisconnection"] = {
      args = {},
      callback = function()
        if self.socket == native and self.proxy == proxy and onDisconnection ~= nil then onDisconnection() end
      end,
    },
    ["OnError"] = {
      args = {},
      callback = function()
        if self.socket == native and self.proxy == proxy and onError ~= nil then onError() end
      end,
    }
  })
  local accepted = native:TryRegisterListener(proxy:Target(), proxy:Function("OnCommand"),
                                               proxy:Function("OnConnection"), proxy:Function("OnDisconnection"))
  if accepted then self.proxy = proxy end
  return accepted == true
end

---@param ipAddress string
---@param port integer
function RedSocket:Connect(ipAddress, port)
  if self:IsRetired() then return false end
  return self.socket:Connect(ipAddress, port)
end

function RedSocket:Disconnect()
  if self:IsRetired() then return false end
  return self.socket:Disconnect()
end

---@param command string single line; no CR/LF
---@param limit integer? retained for compatibility; not a retry count
function RedSocket:SendCommand(command, limit)
  if self:IsRetired() then return false end
  if limit == nil then
    return self.socket:SendCommand(command)
  end
  if self.proxy == nil then return false end
  return self.socket:SendCommand(command, limit, self.proxy:Target(), self.proxy:Function("OnError"))
end

function RedSocket:Destroy()
  local native = self.socket
  if native == nil then return false end
  local forget = self.onDestroy
  -- Revoke ownership and callback admission before invoking native code.
  self.socket = nil
  self.proxy = nil
  self.onDestroy = nil
  if forget ~= nil then forget(self) end
  -- Native Destroy closes/cancels the socket; a separate Disconnect would
  -- queue a redundant lifecycle action. Never automatically retry an
  -- ambiguous native failure on a retired handle.
  local ok = pcall(function() Socket.Destroy(native) end)
  if not ok then print("[DeepBide-RedSocket] Native socket destruction failed") end
  return ok
end

return RedSocket