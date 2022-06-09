package com.mindarray;

public class Constants {

    public static final String DISCOVERY_POST_CHECK = "database.discovery.check";

    public static final String CREDENTIAL_POST_CHECK = "database.credential.check";

    public static final String CREDENTIAL_TABLE = "Credentials";

    public static final String DISCOVERY_NAME = "discovery.name";

    public static final String DISCOVERY_TABLE_NAME = "discovery_name";

    public static final String DISCOVERY_TABLE_ID = "discoveryId";

    public static final String CREDENTIAL_DELETE_NAME_CHECK = "credentials.delete.name.check";

    public static final String CREDENTIAL_TABLE_NAME = "credential_name";

    public static final String TYPE = "type";

    public static final String METHOD = "method";

    public static final String DISCOVERY_TABLE = "Discovery";

    public static final String INVALID_INPUT = "invalid input";

    public static final String IP_ADDRESS = "ip";

    public static final String PORT = "port";

    public static final String USERNAME = "username";

    public static final String PASSWORD = "password";

    public static final String COMMUNITY = "community";

    public static final String VERSION = "version";

    public static final String CONTENT_TYPE = "content-type";

    public static final String CONTENT_VALUE = "application/json";

    public static final String STATUS = "status";

    public static final String ERROR = "error";

    public static final String PROTOCOL = "protocol";

    public static final String SUCCESS = "success";

    public static final String FAIL = "fail";

    public static final String MESSAGE = "message";

    public static final String NOT_PRESENT = "data is not present in database";

    public static final String DISCOVERY_NAME_NOT_UNIQUE = "discovery name is not unique";

    public static final String CREDENTIAL_NAME_NOT_UNIQUE = "credential name is not unique";

    public static final String INVALID_CREDENTIAL_ID = "invalid credential id";

    public static final String RESULT = "result";

    public static final String TABLE_NAME = "tableName";

    public static final String TABLE_COLUMN = "tableColumn";

    public static final String TABLE_ID = "tableId";

    public static final String IN_USE = "Already in use";

    public static final String EVENTBUS_DATABASE = "eventbus.database";

    public static final String DATABASE_ID_CHECK = "database.id.check";

    public static final String DATABASE_INSERT = "database.insert";

    public static final String DATABASE_UPDATE = "database.update";

    public static final String DATABASE_GET = "database.get";

    public static final String DATABASE_DELETE = "database.delete";

    public static final String RUN_DISCOVERY = "run.discovery";

    public static final String RUN_DISCOVERY_INSERT = "run.discovery.insert";

    public static final String MERGE_DATA = "merge.data";

    public static final String PING_FAIL = "ping fail";

    public static final String SSH = "ssh";

    public static final String WINRM = "winrm";

    public static final String SNMP = "snmp";

    public static final String CATEGORY = "category";

    public static final String OBJECTS = "objects";

    public static final String NOT_DISCOVERED = "ip is not discovered yet";

    public static final String MONITOR = "Monitor";

    public static final String MONITOR_ID = "monitorId";

    public static final String EXIST = "already exist";

    public static final String VALIDATE_PROVISION = "validate.provision";

    public static final String MISSING_DATA = "data is missing";

    public static final String CREDENTIAL_ID = "credentialId";

    public static final String NETWORKING = "networking";

    public static final String CREDENTIAL_NAME = "credential.name";

    public static final String USER_METRIC = "UserMetric";

    public static final String INSERT_METRIC = "insert.metric";

    public static final String METRIC_ID = "metricId";

    public static final String METRIC_GROUP = "metricGroup";

    public static final String TIME = "time";

    public static final String DATABASE_DELETE_MONITOR = "database.delete.monitor";

    public static final String HOST = "host";

    public static final String OBJECT_MISSING = "Object field is missing";

    public static final String SCHEDULER = "scheduler";

    public static final String EVENTBUS_POLLER = "eventbus.poller";

    public static final String LINUX = "linux";

    public static final String WINDOWS = "windows";

    public static final String CREATE_CONTEXT = "create.context";

    public static final String POLLING = "polling";

    public static final String POLLER = "Poller";

    public static final String SCHEDULER_DELETE = "scheduler.delete";

    public static final String GETALL = "getall";

    public static final int MIN_POLL_TIME = 60000;

    public static final int MAX_POLL_TIME = 86400000;

    public static final String SCHEDULER_UPDATE = "scheduler.update";

    public static final String LIMIT = "limit";

    public static final String POLLING_ID = "pollerId";

    public static final String DATABASE_GET_POLL_DATA = "database.get.poll.data";

    public static final String DATABASE_UPDATE_GROUP_TIME = "database.update.group.time";

    public static final String TIMESTAMP = "timestamp";

    public static final String DISCOVERY_CATEGORY = "discovery";

    public static final String INVALID_METRIC_GROUP = "Invalid metric group";

    public static final String INVALID_TYPE = "Invalid type";

    public static final String INVALID_PROTOCOL = "Invalid protcol";

    public static final String INVALID_REQUEST = "Invalid request";

    public static final String PORT_MISSING = "Port is missing";

    public static final String INVALID_PORT  = "Port is invalid";

    public static final String INVALID_TIME = "Invalid time";

    public static final String MONITOR_ID_MISSING = "Monitor id is missing";

    public static final String INTERFACE = "interface";

    public static final String PLUGIN_PATH = "./plugin.exe";

    public static final String CPU = "cpu";

    public static final String DISK = "disk";

    public static final String SYSTEM_INFO = "SystemInfo";

    public static final String MEMORY = "memory";

    public static final String PROCESS = "process";

    public static final String PING = "ping";

    public static final String NULL_DATA = "Data is null";

    public static final String TABLE_NAME_EMPTY = "table name is empty";

    public static final String DB_CONNECTION = "jdbc:mysql://localhost:3306/NMS";

    public static final String DB_USER = "root";

    public static final String DB_PASSWORD = "password";

    public static final String MERGE_QUERY = "select discoveryId,port,ip,username,password,type,community,version from Discovery AS D JOIN Credentials AS C ON D.credentialId = C.credentialId where discoveryId='";

    public static final String CREDENTIAL_TABLE_CREATE = "create table if not exists Credentials (credentialId int PRIMARY KEY ,credential_name varchar(255),protocol varchar(255),username varchar(255),password varchar(255),community varchar(255),version varchar(255))";

    public static final String MONITOR_TABLE_CREATE = "create table if not exists Monitor (monitorId int PRIMARY KEY ,ip varchar(255),type varchar(255),port int,host varchar(255))";

    public static final String DISCOVERY_TABLE_CREATE = "create table if not exists Discovery (discoveryId int PRIMARY KEY ,credentialId int,discovery_name varchar(255),ip varchar(255),type varchar(255),port int,result JSON)";

    public static final String USER_METRIC_TABLE_CREATE = "create table if not exists UserMetric (metricId int PRIMARY KEY ,monitorId int,credentialId int,metricGroup varchar(255),time int,objects JSON)";

    public static final String POLLER_TABLE_CREATE = "create table if not exists Poller (pollerId int PRIMARY KEY AUTO_INCREMENT , monitorId int, metricGroup varchar(255) ,result json,timestamp DATETIME)";

    public static final String EMPTY_DATA =  "Empty data from plugin";

}
