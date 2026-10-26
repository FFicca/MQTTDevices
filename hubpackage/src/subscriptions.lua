--[[
  Copyright 2022 Todd Austin

  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
  except in compliance with the License. You may obtain a copy of the License at:

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software distributed under the
  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
  either express or implied. See the License for the specific language governing permissions
  and limitations under the License.


  DESCRIPTION

  MQTT Device Driver - Subscriptions-related Functions

--]]

local log = require "log"
local socket = require "cosock.socket"


local function build_html(list)

  local html_list = ''

  for _, item in ipairs(list) do
    html_list = html_list .. '<tr><td>' .. item .. '</td></tr>\n'
  end

  local html =  {
                  '<!DOCTYPE html>\n',
                  '<HTML>\n',
                  '<HEAD>\n',
                  '<style>\n',
                  'table, td {\n',
                  '  border: 1px solid black;\n',
                  '  border-collapse: collapse;\n',
                  '  font-size: 12px;\n',
                  '  padding: 3px;\n',
                  '}\n',
                  '</style>\n',
                  '</HEAD>\n',
                  '<BODY>\n',
                  '<table>\n',
                  html_list,
                  '</table>\n',
                  '</BODY>\n',
                  '</HTML>\n'
                }
    
  return (table.concat(html))
end


-- Find all devices that subscribe to the given topic (primary or switch topic)
local function determine_devices(targettopic)

  local targetlist = {}
  local found_ids = {}
  local devicelist = thisDriver:get_devices()

  for id, topic in pairs(SUBSCRIBED_TOPICS) do
    if topic == targettopic then
      for _, device in ipairs(devicelist) do
        if device.id == id then
          table.insert(targetlist, device)
          found_ids[id] = true
        end
      end
    end
  end

  -- Also resolve devices subscribed via their dedicated switch topic
  for id, topic in pairs(SWITCH_TOPICS) do
    if topic == targettopic and not found_ids[id] then
      for _, device in ipairs(devicelist) do
        if device.id == id then
          table.insert(targetlist, device)
          found_ids[id] = true
        end
      end
    end
  end

  return targetlist

end


local function is_subscribed(qtopic)

  for _, topic in pairs(SUBSCRIBED_TOPICS) do
    if topic == qtopic then; return true; end
  end
  return false

end


local function unique_topic_list()

  local list = {}

  local function add_if_new(topic)
    for _, item in ipairs(list) do
      if item == topic then return end
    end
    table.insert(list, topic)
  end

  for _, topic in pairs(SUBSCRIBED_TOPICS) do
    add_if_new(topic)
  end

  -- Include switch topics in the display list
  for _, topic in pairs(SWITCH_TOPICS) do
    add_if_new(topic)
  end

  return list

end


local function subscribe_topic(device)

  if not client or not client.connection then
    log.debug(string.format('Skipping subscribe for <%s>; MQTT client is not yet connected', device.label))
    return false
  end

  device:set_field('mqtt_last_subscribe_ts', socket.gettime())
  device:set_field('mqtt_warned_motion_switch_overlap', false)
  device:set_field('mqtt_warned_optimistic_retained', false)

  if is_subscribed(device.preferences.subTopic) then
    log.debug ('Already subscribed to topic', device.preferences.subTopic)
    SUBSCRIBED_TOPICS[device.id] = device.preferences.subTopic
    device:emit_event(cap_status.status('Subscribed'))
    return true
  else
    SUBSCRIBED_TOPICS[device.id] = device.preferences.subTopic
    assert(client:subscribe{ topic=device.preferences.subTopic, qos=1, callback=function(suback)
      log.info(string.format("Device <%s> subscribed to %s: %s", device.label, device.preferences.subTopic, suback))
      
      creator_device:emit_event(cap_topiclist.topiclist(build_html(unique_topic_list())))
      device:emit_event(cap_status.status('Subscribed'))

    end})
    return true
  end

end


local function subscribe_switch_topic(device)
  -- Subscribe a Motion-Switch device to its dedicated switch topic if configured
  local dtype = device.device_network_id:match('MQTT_(.+)_+')
  if dtype ~= 'Motion-Switch' then return end

  local sw_topic = device.preferences.switchSubTopic
  if (sw_topic == nil) or (sw_topic == '') or (sw_topic == 'xxxxx/xxxxx') then return end

  if sw_topic == device.preferences.subTopic then
    log.debug(string.format('Device <%s> switchSubTopic same as subTopic; no extra subscription needed', device.label))
    SWITCH_TOPICS[device.id] = nil
    return
  end

  SWITCH_TOPICS[device.id] = sw_topic

  -- Only subscribe at MQTT level if not already subscribed (another device may share it)
  local already = false
  for _, topic in pairs(SUBSCRIBED_TOPICS) do
    if topic == sw_topic then already = true; break end
  end
  for id2, topic in pairs(SWITCH_TOPICS) do
    if topic == sw_topic and id2 ~= device.id then already = true; break end
  end

  if not already then
    assert(client:subscribe{ topic=sw_topic, qos=1, callback=function(suback)
      log.info(string.format("Device <%s> switch topic subscribed to %s: %s", device.label, sw_topic, suback))
      creator_device:emit_event(cap_topiclist.topiclist(build_html(unique_topic_list())))
    end})
  end

end


local function subscribe_all()

  local devicelist = thisDriver:get_devices()
  for _, device in ipairs(devicelist) do
    if not device.device_network_id:find('Master', 1, 'plaintext') then
      if (device.preferences.subTopic ~= 'xxxxx/xxxxx') and (device.preferences.subTopic ~= nil) then
        subscribe_topic(device)
        subscribe_switch_topic(device)
      end
    end
  end
end


local function unsubscribe(id, topic, delete_flag)

  local qty_check_val = 1                                 -- =1 if device changing subscription; =0 if device was deleted
  if delete_flag == true then; qty_check_val = 0; end

  if #determine_devices(topic) == qty_check_val then      -- unsubscribe only if no more devices using this topic

    local rc, err = client:unsubscribe{ topic=topic, callback=function(unsuback)
          log.info("\t\tUnsubscribe callback:", unsuback)
      end}
      
    if rc == false then
      log.debug ('\tUnsubscribe failed with err:', err)
    else
      log.debug (string.format('\tUnsubscribed from %s', topic))
      SUBSCRIBED_TOPICS[id] = nil
      creator_device:emit_event(cap_topiclist.topiclist(build_html(unique_topic_list())))
    end
  else
    log.debug (string.format('Subscription to <%s> still in use by another device', topic))
    SUBSCRIBED_TOPICS[id] = nil
  end

  -- Also unsubscribe from dedicated switch topic if this device has one
  local sw_topic = SWITCH_TOPICS[id]
  if sw_topic then
    if #determine_devices(sw_topic) == qty_check_val then
      local rc2, err2 = client:unsubscribe{ topic=sw_topic, callback=function(unsuback)
        log.info("\t\tSwitch topic unsubscribe callback:", unsuback)
      end}
      if rc2 == false then
        log.debug ('\tSwitch topic unsubscribe failed with err:', err2)
      else
        log.debug (string.format('\tUnsubscribed from switch topic %s', sw_topic))
      end
    else
      log.debug (string.format('Switch subscription to <%s> still in use by another device', sw_topic))
    end
    SWITCH_TOPICS[id] = nil
    creator_device:emit_event(cap_topiclist.topiclist(build_html(unique_topic_list())))
  end

end


local function unsubscribe_all()

  local sublist = SUBSCRIBED_TOPICS

  for id, topic in pairs(sublist) do
    unsubscribe(id, topic)
  end

  SWITCH_TOPICS = {}

end


local function get_subscribed_topic(device)

  for id, topic in pairs(SUBSCRIBED_TOPICS) do
    if id == device.id then
      return id, topic
    end
  end
end


local function mqtt_subscribe(device)

  if client then

    local id, topic = get_subscribed_topic(device)

    if topic then
      log.debug (string.format('Unsubscribing device <%s> from %s', device.label, topic))
      unsubscribe(id, topic)
    end

    subscribe_topic(device)
    subscribe_switch_topic(device)
  end
end


return{
          determine_devices = determine_devices,
          subscribe_topic = subscribe_topic,
          subscribe_all = subscribe_all,
          mqtt_subscribe = mqtt_subscribe,
          unsubscribe = unsubscribe,
unsubscribe_all = unsubscribe_all,
          get_subscribed_topic = get_subscribed_topic,
            subscribe_switch_topic = subscribe_switch_topic,
}
