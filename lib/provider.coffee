redis = require("haredis")

module.exports = exports = class
  constructor: (hosts, timeout = 1250) ->
    @logger  = console
    @timeout = parseInt(timeout, 10) || 1250

    hosts ||= ["127.0.0.1"]
    hosts   = [hosts] unless hosts instanceof Array

    for host, i in hosts
      host = "#{host}:6379" unless host.indexOf(":") isnt -1
      hosts[i] = host

    @client = redis.createClient(hosts, detect_buffers: yes)

    @logger.log "Redis client with hosts", hosts

  setOwner: (owner) =>
    @owner  = owner
    @logger = @owner.log

  get: (key, cb) ->
    logger    = @logger
    timeoutMs = @timeout

    returned = no
    returnFn = (error, data) ->
      return if returned
      returned = yes
      cb(error, data)

    abortFn = ->
      logger.error "!! Redis provider ABORTING get -- didn't get an answer within #{timeoutMs} ms"
      returnFn("Redis didn't respond in a timely manner")

    timeout = setTimeout(abortFn, @timeout)

    @client.get key, (error, data) ->
      clearTimeout(timeout)
      error = "Key doesnt exist" if not error and data is null
      returnFn(error, data if not error)

  set: (key, value, expires = 0) ->
    # We don't care about expiry ATM
    logger  = @logger

    @client.set key, value, (error, status) ->
      logger.error "!! Redis provider SET failed: #{error.toString()}" if error