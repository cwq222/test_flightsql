package com.portofino.arrow

import org.apache.arrow.adbc.core.AdbcConnection
import org.apache.arrow.adbc.core.AdbcDatabase
import org.apache.arrow.adbc.core.AdbcDriver
import org.apache.arrow.adbc.core.AdbcStatement
import org.apache.arrow.adbc.driver.flightsql.FlightSqlDriver
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.DateDayVector
import org.apache.arrow.vector.types.Types
import org.apache.arrow.flight.FlightClient
import org.apache.arrow.flight.Location
import org.apache.arrow.flight.FlightStream
import org.apache.arrow.flight.auth2.{BasicAuthCredentialWriter, ClientBearerHeaderHandler, ClientIncomingAuthHeaderMiddleware}
import org.apache.arrow.flight.grpc.CredentialCallOption
import org.apache.arrow.flight.client.ClientCookieMiddleware
import org.apache.arrow.flight.sql.FlightSqlClient
import java.sql.DriverManager
import java.time.LocalDate
import java.util.{HashMap, Properties}
import scala.collection.JavaConverters._

/**
 * Arrow Flight SQL 测试程序
 * 测试通过 Arrow Flight SQL JDBC 驱动、ADBC + FlightSQL协议 连接 Doris 并读取数据
 * @author jiangxintong@chinamobile.com 2026/1/28
 */
object ArrowFlightSQLSuite {

  def main(args: Array[String]): Unit = {
    println("=" * 60)
    println("Arrow Flight SQL Test")
    println("=" * 60)

    // 从配置文件加载参数
    val props = new Properties()
    val inputStream = getClass.getResourceAsStream("/doris.properties")
    if (inputStream != null) {
      props.load(inputStream)
      inputStream.close()
    } else {
      println("Warning: doris.properties not found in classpath, using default values.")
    }
    val host = props.getProperty("host")
    val port = props.getProperty("port")
    val mysqlPort = props.getProperty("mysql.port")
    val user = props.getProperty("user")
    val password = props.getProperty("password")
    val query = props.getProperty("query")
    try {
      // 第一步：测试直接 JDBC+Flightsql 连接
      println("\n" + "=" * 60)
      println("Testing JDBC+Flightsql Connection")
      println("=" * 60)
      try {
        testJdbcConnection(host, port, user, password, query)
        println("\n✓ JDBC+Flightsql测试完成，所有连接已关闭")
      } catch {
        case e: Exception =>
          println(s"✗ JDBC+Flightsql测试失败: ${e.getMessage}")
          e.printStackTrace()
      }
      // 第二步：测试 ADBC+Flightsql
      println("\n" + "=" * 60)
      println("Testing ADBC Native API")
      println("=" * 60)
      try {
        testAdbcNativeFlightSQL(host, port, user, password, query)
        println("\n✓ ADBC+Flightsql测试完成，所有连接已关闭")
      } catch {
        case e: Exception =>
          println(s"✗ ADBC+Flightsql测试失败: ${e.getMessage}")
          e.printStackTrace()
      }
      // 第三步：测试 DirectArrowFlight
      println("\n" + "=" * 60)
      println(" Testing DirectArrowFlight ")
      println("=" * 60)
      try {
        testDirectArrowFlight(host, port, user, password, query)
        println("\n✓ DirectArrowFlight测试完成，所有连接已关闭")
      } catch {
        case e: Exception =>
          println(s"✗ DirectArrowFlight测试失败: ${e.getMessage}")
          e.printStackTrace()
      }

      println("\n" + "=" * 60)
      println("所有测试完成")
      println("=" * 60)
    } catch {
      case e: Exception =>
        println(s"Error: ${e.getMessage}")
        e.printStackTrace()
        System.exit(1)
    }
  }
  /**
   * 测试JDBC+Flightsql 连接
   */
  def testJdbcConnection(host: String, port: String, user: String, password: String, query: String): Unit = {
    println("Testing JDBC+Flightsql Connection...")

    // 加载驱动
    Class.forName("org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver")
    // 构建连接 URL
    val url = s"jdbc:arrow-flight-sql://$host:$port" +
      "?useServerPrepStmts=false&cachePrepStmts=false&useSSL=false&useEncryption=false"
    println(s"URL: $url")
    // 创建连接属性
    val props = new Properties()
    props.setProperty("user", user)
    props.setProperty("password", password)

    // 连接并查询
    val connection = DriverManager.getConnection(url, props)
    try {
      println("✓ Connection established")
      val statement = connection.createStatement()
      try {
        println(s"Executing query: $query")
        val queryStart = System.currentTimeMillis()
        val resultSet = statement.executeQuery(query)
        val metadata = resultSet.getMetaData
        val columnCount = metadata.getColumnCount

        // 打印列信息
        println("\nColumns:")
        for (i <- 1 to columnCount) {
          println(s"  ${metadata.getColumnName(i)}: ${metadata.getColumnTypeName(i)}")
        }

        // 打印数据：读取全表所有行，仅打印前10行
        println("\nData:")
        var printRowCount = 0 // 控制仅打印前10行
        var totalRowCount = 0L // 统计全表总行数
        // 仅保留resultSet.next()，循环读取全表所有行
        while (resultSet.next()) {
          totalRowCount += 1 // 每读一行，总行数+1（必执行，统计全表）
          // 仅前10行执行打印逻辑
          if (printRowCount < 10) {
            val values = (1 to columnCount).map { i =>
              val value = resultSet.getObject(i)
              if (resultSet.wasNull()) "NULL" else value.toString
            }
            println(s"  ${values.mkString(", ")}")
            printRowCount += 1 // 打印行计数，到10后不再打印
          }
        }
        val queryEnd = System.currentTimeMillis()
        val cost = queryEnd - queryStart
        // 打印结果：补充全表总行数，耗时为读取全表的真实用时
        println(s"\n✓ Query executed successfully (total rows: $totalRowCount, showing first $printRowCount rows)")
        println(s"✓ JDBC+Flightsql 读取全表总耗时: $cost 毫秒(ms)")

      } finally {
        statement.close()
      }
    } finally {
      connection.close()
    }
  }

  /**
   * 测试 ADBC  + Arrow Flight SQL 读取 Doris
   */
  def testAdbcNativeFlightSQL(host: String, port: String, user: String, password: String, query: String): Unit = {
    println("Testing ADBC+Flight SQL")
    // 1. 初始化 Arrow 内存分配器（必须关闭，避免内存泄漏）
    var allocator: BufferAllocator = null
    var database: AdbcDatabase = null
    var connection: AdbcConnection = null
    var statement: AdbcStatement = null
    var queryResult: AdbcStatement.QueryResult = null

    try {
      // 初始化内存分配器
      allocator = new RootAllocator(Long.MaxValue)
      println("✓ Arrow BufferAllocator initialized")

      // 2. 构建 ADBC Flight SQL 驱动配置
      val adbcConfig = new HashMap[String, Object]()
      AdbcDriver.PARAM_URI.set(adbcConfig, s"grpc://$host:$port")
      AdbcDriver.PARAM_USERNAME.set(adbcConfig, user)
      AdbcDriver.PARAM_PASSWORD.set(adbcConfig, password)

      // 3. 加载 ADBC Flight SQL 驱动并创建数据库连接
      val driver = new FlightSqlDriver(allocator)
      database = driver.open(adbcConfig)
      connection = database.connect()
      println(s"✓ ADBC Flight SQL connection established (连接成功)")

      // 4. 创建 Statement 并执行查询
      statement = connection.createStatement()
      statement.setSqlQuery(query)
      println(s"✓ Executing query: $query")
      val queryStart = System.currentTimeMillis()
      queryResult = statement.executeQuery()

      // 5. 获取 ArrowReader 来读取结果集
      val reader = queryResult.getReader
      val schemaRoot = reader.getVectorSchemaRoot

      // 6. 打印 Schema 信息
      println("\nArrow Schema:")
      val fields = schemaRoot.getSchema.getFields.asScala
      fields.foreach { field =>
        val fieldType = Types.getMinorTypeForArrowType(field.getType)
        println(s"  ${field.getName}: $fieldType")
      }

      // 7. 读取所有数据（统计总行数，只打印前 10 行）
      println("\nData (top 10 rows):")
      var totalRowCount = 0L
      var printedRows = 0
      while (reader.loadNextBatch()) {
        val batchRowCount = schemaRoot.getRowCount
        for (i <- 0 until batchRowCount) {
          if (printedRows < 10) {
            val rowValues = fields.map { field =>
              val vector = schemaRoot.getVector(field.getName)
              val value = vector.getObject(i)
              if (value == null) "NULL" else vector match {
                case _: DateDayVector => LocalDate.ofEpochDay(value.asInstanceOf[Int].toLong).toString
                case _ => value.toString
              }
            }
            println(s"  ${rowValues. mkString(", ")}")
            printedRows +=  1
          }
          totalRowCount += 1
        }
      }
      val queryEnd = System.currentTimeMillis()
      val cost = queryEnd - queryStart
      println(s"查询成功，总行数: $totalRowCount")
      println(s"✓ ADBC+Flightsql 数据读取总耗时: $cost 毫秒(ms)")
    } catch {
      case e: Exception =>
        println(s"❌ ADBC Native API failed: ${e.getMessage}")
        throw e
    } finally {
      // 8. 逆序关闭资源，避免内存泄漏
      if (queryResult != null) queryResult.close()
      if (statement != null) statement.close()
      if (connection != null) connection.close()
      if (database != null) database.close()
      if (allocator != null) allocator.close()
      println("✓ ADBC resources closed successfully")
    }
  }

  /**
   * 测试 FlightClient 读取 Doris
   */
  def testDirectArrowFlight(host: String, port: String, user: String, password: String, query: String): Unit = {
    println("Testing Direct Arrow Flight SQL Access...")

    // 1. 初始化内存分配器
    var allocator: BufferAllocator = null
    var flightClient: FlightClient = null
    var sqlClient: FlightSqlClient = null
    var beClient: FlightClient = null
    var stream: FlightStream = null

    try {
      allocator = new RootAllocator(Long.MaxValue)
      println("✓ Arrow BufferAllocator initialized")

      // 2. 创建 FlightClient 连接 FE（注册认证中间件 + Cookie中间件）
      val location = Location.forGrpcInsecure(host, port.toInt)
      val authFactory = new ClientIncomingAuthHeaderMiddleware.Factory(new ClientBearerHeaderHandler())
      val cookieFactory = new ClientCookieMiddleware.Factory()
      flightClient = FlightClient.builder(allocator, location)
        .intercept(authFactory)
        .intercept(cookieFactory)
        .build()
      println("✓ FlightClient created")

      // 3. Handshake 认证（Basic Auth）
      val credentialCallOption = new CredentialCallOption(new BasicAuthCredentialWriter(user, password))
      flightClient.handshake(credentialCallOption)
      val bearerToken = authFactory.getCredentialCallOption
      println("✓ Authentication successful")

      // 4. 创建 FlightSqlClient 并执行查询（FE 负责查询规划）
      sqlClient = new FlightSqlClient(flightClient)
      println(s"Executing query: $query")
      val queryStart = System.currentTimeMillis()
      val flightInfo = sqlClient.execute(query, bearerToken)

      // 5. 从 FlightInfo 中获取 endpoint，解析 BE 节点地址
      val endpoint = flightInfo.getEndpoints.asScala.head
      val ticket = endpoint.getTicket
      val locations = endpoint.getLocations.asScala

      // 6. 连接 BE 节点获取数据（Doris FE/BE 分离架构）
      if (locations.nonEmpty) {
        val beLocation = locations.head
        println(s"✓ Connecting to BE endpoint: $beLocation")
        val beAuthFactory = new ClientIncomingAuthHeaderMiddleware.Factory(new ClientBearerHeaderHandler())
        val beCookieFactory = new ClientCookieMiddleware.Factory()
        beClient = FlightClient.builder(allocator, beLocation)
          .intercept(beAuthFactory)
          .intercept(beCookieFactory)
          .build()
        beClient.handshake(new CredentialCallOption(new BasicAuthCredentialWriter(user, password)))
        val beToken = beAuthFactory.getCredentialCallOption
        println("✓ BE Authentication successful")
        stream = beClient.getStream(ticket, beToken)
      } else {
        stream = sqlClient.getStream(ticket, bearerToken)
      }
      val schemaRoot = stream.getRoot()

      // 7. 打印 Schema 信息
      println("\nArrow Schema:")
      val fields = schemaRoot.getSchema().getFields().asScala
      fields.foreach { field =>
        val fieldType = Types.getMinorTypeForArrowType(field.getType())
        println(s"  ${field.getName()}: $fieldType")
      }

      // 8. 读取所有数据（列式方式）
      println("\nData (top 10 rows):")
      var totalRowCount = 0L
      var printedRows = 0

      while (stream.next()) {
        val batchRowCount = schemaRoot.getRowCount()
        for (i <- 0 until batchRowCount) {
          if (printedRows < 10) {
            val rowValues = fields.map { field =>
              val vector = schemaRoot.getVector(field.getName())
              val value = vector.getObject(i)
              if (value == null) "NULL" else vector match {
                case _: DateDayVector => LocalDate.ofEpochDay(value.asInstanceOf[Int].toLong).toString
                case _ => value.toString
              }
            }
            println(s"  ${rowValues.mkString(", ")}")
            printedRows += 1
          }
          totalRowCount += 1
        }
      }
      val queryEnd = System.currentTimeMillis()
      val cost = queryEnd - queryStart
      println(s"查询成功，总行数: $totalRowCount")
      println(s"✓ Direct Arrow Flight SQL 数据读取总耗时: $cost 毫秒(ms)")
    } catch {
      case e: Exception =>
        println(s"❌ Direct Arrow Flight SQL failed: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      // 逆序关闭资源
      if (stream != null) stream.close()
      if (beClient != null) beClient.close()
      if (sqlClient != null) sqlClient.close()
      if (flightClient != null) flightClient.close()
      if (allocator != null) allocator.close()
      println("✓ Direct Arrow Flight SQL resources closed successfully")
    }
  }
}
