-- MIT: original RedSocket by Louis Poirier; modifications by DeepBide.
local RedSocket = require("RedSocket")
local sockets = {}
local closing = false

local function forget(socket)
  sockets[socket] = nil
end

local api = {
  version = "0.5.2-rc.3",
  -- Kept under GetMod("RedSocket") for existing CET consumers.
  createSocket = function()
    if closing then return nil end
    -- Native Running stop can retire wrappers before CET shutdown.
    for socket in pairs(sockets) do
      if socket:IsRetired() then socket:Destroy() end
    end
    local socket = RedSocket:new(forget)
    if socket ~= nil then sockets[socket] = true end
    return socket
  end,

  destroySocket = function(socket)
    if socket == nil or not sockets[socket] then return false end
    -- Destroy retires the registry entry before invoking native shutdown.
    return socket:Destroy()
  end
}

registerForEvent("onShutdown", function()
  if closing then return end
  closing = true
  local owned = sockets
  sockets = {}
  for socket in pairs(owned) do
    -- A native failure cannot abandon the other independent sockets.
    socket:Destroy()
  end
end)

return api
