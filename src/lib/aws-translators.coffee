_ = require('lodash')
dataTrans = require('./data-translators')
Promise = require('bluebird')
debug = require('debug')('dynasty:aws-translators')

buildFilters = (target, filters) ->
  if filters
    scanFilterFunc(target, filter) for filter in filters

scanFilterFunc = (target, filter) ->
  target[filter.column] =
    ComparisonOperator: filter.op || 'EQ'
    AttributeValueList: [{}]
  target[filter.column].AttributeValueList[0][filter.type || 'S'] = filter.value
  target

buildExclusiveStartKey = (awsParams, params) ->
  if params.exclusiveStartKey
    awsValue = {}
    for key, value of params.exclusiveStartKey
      awsValue[key] = dataTrans.toDynamo(params.exclusiveStartKey[key])
    awsParams.ExclusiveStartKey = awsValue

module.exports.processAllPages = (deferred, dynamo, functionName, params)->

  stats =
    Count: 0

  resultHandler = (err, result) ->
    if err then return deferred.reject(err)

    deferred.notify dataTrans.fromDynamo result.Items
    stats.Count += result.Count
    if result.LastEvaluatedKey
      params.ExclusiveStartKey = result.LastEvaluatedKey
      dynamo[functionName] params, resultHandler
    else
      deferred.resolve stats

  dynamo[functionName] params, resultHandler
  deferred.promise


module.exports.getKeySchema = (tableDescription) ->
  getKeyAndType = (keyType) ->
    keyName = _.find tableDescription.Table.KeySchema, (key) ->
      key.KeyType is keyType
    ?.AttributeName

    keyDataType = _.find tableDescription.Table.AttributeDefinitions,
    (attribute) ->
      attribute.AttributeName is keyName
    ?.AttributeType
    [keyName, keyDataType]

  [hashKeyName, hashKeyType] = getKeyAndType 'HASH'
  [rangeKeyName, rangeKeyType] = getKeyAndType 'RANGE'

  hashKeyName: hashKeyName
  hashKeyType: hashKeyType
  rangeKeyName: rangeKeyName
  rangeKeyType: rangeKeyType

getKey = (params, keySchema) ->
  if !_.isObject params
    params = hash: params+''

  key = {}
  key[keySchema.hashKeyName] = {}
  key[keySchema.hashKeyName][keySchema.hashKeyType] = params.hash+''

  if params.range
    key[keySchema.rangeKeyName] = {}
    key[keySchema.rangeKeyName][keySchema.rangeKeyType] = params.range+''

  key

module.exports.deleteItem = (params, options, callback, keySchema) ->
  # console.log 'param,options', params, options

  key = getKey(params, keySchema)
  # console.log 'key from params an keyschema = ', key
  expressionAttributeValues = {}
  # Allow setting arbitrary attribute values
  if options?.expressionAttributeValues
    expressionAttributeValues = _.mapValues options.expressionAttributeValues, (value, key) -> dataTrans.toDynamo(value)
    # expressionAttributeValues = _.mapKeys expressionAttributeValues, (value, key) ->
      # return options.conditionExpression.split(' = ',1)[0].replace('#',':')

  # Setup ExpressionAttributeNames mapping key -> #key so we don't bump into
  # reserved words
  expressionAttributeNames = {}
  expressionAttributeNames["##{key.replace(':','')}"] = key.replace(':','') for key, i in Object.keys(expressionAttributeValues)

  conditionExpression = options.conditionExpression

  awsParams =
    TableName: @name
    Key: getKey(params, keySchema)
    ExpressionAttributeNames: expressionAttributeNames
    ExpressionAttributeValues: expressionAttributeValues
    ConditionExpression: conditionExpression

  # console.log awsParams

  if options?.conditionExpression
    awsParams.ConditionExpression = options.conditionExpression
  @parent.dynamo.deleteItemAsync(awsParams)

  # awsParams =
  #   TableName: @name
  #   Key: getKey(params, keySchema)
  # @parent.dynamo.deleteItemAsync awsParams

module.exports.batchGetItem = (params, callback, keySchema) ->
  awsParams = {}
  awsParams.RequestItems = {}
  name = @name
  awsParams.RequestItems[@name] = Keys: _.map(params, (param) -> getKey(param, keySchema))
  @parent.dynamo.batchGetItemAsync(awsParams)
    .then (data) ->
      dataTrans.fromDynamo(data.Responses[name])
    .nodeify(callback)

module.exports.getItem = (params, options, callback, keySchema) ->
  awsParams =
    TableName: @name
    Key: getKey(params, keySchema)

  @parent.dynamo.getItemAsync(awsParams)
    .then (data)->
      dataTrans.fromDynamo(data.Item)
    .nodeify(callback)

module.exports.queryByHashKey = (key, callback, keySchema) ->
  awsParams =
    TableName: @name
    KeyConditions: {}

  hashKeyName = keySchema.hashKeyName
  hashKeyType = keySchema.hashKeyType

  awsParams.KeyConditions[hashKeyName] =
    ComparisonOperator: 'EQ'
    AttributeValueList: [{}]
  awsParams.KeyConditions[hashKeyName].AttributeValueList[0][hashKeyType] = key

  @parent.dynamo.queryAsync(awsParams)
    .then (data) ->
      dataTrans.fromDynamo(data.Items)
    .nodeify(callback)

module.exports.scan = (params, options, callback, keySchema) ->
  params ?= {}
  awsParams =
    TableName: @name
    ScanFilter: {}
    Limit: params.limit
    TotalSegments: params.totalSegment
    Segment: params.segment

  awsParams.AttributesToGet = params.attrsGet if params.attrsGet

  buildFilters(awsParams.ScanFilter, params.filters)

  @parent.dynamo.scanAsync(awsParams)
    .then (data)->
      dataTrans.fromDynamo(data.Items)
    .nodeify(callback)

module.exports.scanPaged = (params, options, callback, keySchema) ->
  params ?= {}
  awsParams =
    TableName: @name
    ScanFilter: {}
    Limit: params.limit
    TotalSegments: params.totalSegment
    Segment: params.segment

  awsParams.AttributesToGet = params.attrsGet if params.attrsGet


  buildExclusiveStartKey(awsParams, params)
  buildFilters(awsParams.ScanFilter, params.filters)

  @parent.dynamo.scanAsync(awsParams)
    .then (data)->
      lastEvaluatedKey = dataTrans.fromDynamo(data.LastEvaluatedKey)
      res =
        items: dataTrans.fromDynamo(data.Items)
        count: data.Count
      if lastEvaluatedKey
        res.lastEvaluatedKey = lastEvaluatedKey
      res
    .nodeify(callback)

module.exports.scanAll = (params, options, callback, keySchema) ->
  items = []
  page = 0
  scanNext = (exclusiveStartKey) =>
    page++
    if exclusiveStartKey?
      options.exclusiveStartKey = exclusiveStartKey
    #console.log("Scanning page #{page} (#{items.length} fetched so far)")
    @scanPaged(params, options, callback)
      .then (res) ->
        items = items.concat(res.items)
        if res.lastEvaluatedKey
          scanNext(exclusiveStartKey)
        else
          items
  scanNext()

module.exports.query = (params, options, callback, keySchema) ->
  params ?= {}
  awsParams =
    TableName: @name
    IndexName: params.indexName
    KeyConditions: {}
    QueryFilter: {}
  awsParams.AttributesToGet = params.attrsGet if params.attrsGet

  buildFilters(awsParams.KeyConditions, params.keyConditions)
  buildFilters(awsParams.QueryFilter, params.filters)

  @parent.dynamo.queryAsync(awsParams)
    .then (data) ->
      dataTrans.fromDynamo(data.Items)
    .nodeify(callback)

module.exports.queryPaged = (params, options, callback, keySchema) ->
  params ?= {}
  awsParams =
    TableName: @name
    IndexName: params.indexName
    KeyConditions: {}
    QueryFilter: {}
  awsParams.AttributesToGet = params.attrsGet if params.attrsGet

  buildExclusiveStartKey(awsParams, params)
  buildFilters(awsParams.KeyConditions, params.keyConditions)
  buildFilters(awsParams.QueryFilter, params.filters)



  @parent.dynamo.queryAsync(awsParams)
    .then (data)->
      lastEvaluatedKey = dataTrans.fromDynamo(data.LastEvaluatedKey)
      res =
        items: dataTrans.fromDynamo(data.Items)
        count: data.Count
      if lastEvaluatedKey
        res.lastEvaluatedKey = lastEvaluatedKey
      res
    .nodeify(callback)


module.exports.putItem = (obj, options, callback) ->
  awsParams =
    TableName: @name
    Item: _.transform(obj, (res, val, key) ->
      res[key] = dataTrans.toDynamo(val))

  @parent.dynamo.putItemAsync(awsParams)

module.exports.updateItem = (params, obj, options, callback, keySchema) ->
  key = getKey(params, keySchema)

  # Set up the Expression Attribute Values map.
  expressionAttributeValues = _.mapKeys obj, (value, key) -> return ':' + key
  expressionAttributeValues = _.mapValues expressionAttributeValues, (value, key) -> dataTrans.toDynamo(value)
  # Allow setting arbitrary attribute values
  if options?.expressionAttributeValues
    options.expressionAttributeValues = _.mapValues options.expressionAttributeValues, (value, key) -> dataTrans.toDynamo(value)
    _.extend(expressionAttributeValues, options.expressionAttributeValues)

  # Setup ExpressionAttributeNames mapping key -> #key so we don't bump into
  # reserved words
  expressionAttributeNames = {}
  expressionAttributeNames["##{key}"] = key for key, i in Object.keys(obj)

  # Set up the Update Expression
  action = if options?.incrementNumber then 'ADD' else 'SET'
  calcUpdateExpression = (value, key) ->
    if options?.incrementNumber
      "##{key} :#{key}"
    else
      "##{key} = :#{key}"
  updateExpression = "#{action} " + _.keys(_.mapKeys obj, calcUpdateExpression).join ','

  awsParams =
    TableName: @name
    Key: getKey(params, keySchema)
    ExpressionAttributeNames: expressionAttributeNames
    ExpressionAttributeValues: expressionAttributeValues
    UpdateExpression: updateExpression

  if options?.conditionExpression
    awsParams.ConditionExpression = options.conditionExpression
  @parent.dynamo.updateItemAsync(awsParams)
