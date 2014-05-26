redis = require("redis")
Pool  = require("generic-pool").Pool

module.exports = exports = class
  constructor: (hosts) ->
    hosts ||= ["127.0.0.1"]

    for host, i in hosts
      host = "#{host}:6379" unless host.indexOf(":") isnt -1
      hosts[i] = host

    primaryHost = hosts[0].split(":")

    @pool = Pool
      name: "redis"
      max:  100
      idleTimeoutMillis: 600000
      log: no

      create: (cb) ->
        client = redis.createClient(primaryHost[1], primaryHost[0], detect_buffers: yes)
        client.on "ready", ->
          cb(null, client)

      destroy: (client) ->
        client.end()

  get: (key, cb) ->
    pool = @pool
    pool.acquire (error, client) ->
      if error or not client
        error = "!! Redis provider: Failed to acquire client during GET (#{error})"
        console.error error
        cb(error)
      else
        client.get key, (error, data) ->
          error = "Key doesnt exist" if not error and data is null
          pool.release(client)
          cb(error, data if not error)

  set: (key, value, expires = 0) ->
    expires = parseInt(expires, 10) || 0
    pool = @pool
    pool.acquire (error, client) ->
      if error
        console.error "!! Redis provider: Failed to acquire client during SET (#{error})"
      else
        client.set key, value, (error, status) ->
          pool.release(client)
          console.error "!! Redis provider SET failed: #{error.toString()}" if error