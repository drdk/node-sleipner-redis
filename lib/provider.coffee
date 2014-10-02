redis = require("haredis")

module.exports = exports = class
  constructor: (options = {}) ->
    @owner =
      logger: console

    @timeout = if options.timeout? then parseInt(options.timeout, 10) else 1250

    options.disableHARedisLogging = if options.disableHARedisLogging? then options.disableHARedisLogging else false

    options.hosts ||= ["127.0.0.1"]
    options.hosts   = [options.hosts] unless options.hosts instanceof Array

    for host, i in options.hosts
      host = "#{host}:6379" unless host.indexOf(":") isnt -1
      options.hosts[i] = host

    redis.debug_mode = true
    @client = redis.createClient(options.hosts, detect_buffers: yes)

    if options.auth?
      authCallback = if options.authCallback? then options.authCallback else null
      @client.auth(options.auth, authCallback)

    if options.disableHARedisLogging
      @client.debug = dummyFn
      @client.info  = dummyFn
      @client.warn  = dummyFn
      @client.error = dummyFn

  get: (key, cb) ->
    logger    = @owner.logger
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
    logger  = @owner.logger

    @client.set key, value, (error, status) ->
      logger.error "!! Redis provider SET failed: #{error.toString()}" if error

  dummyFn = ->