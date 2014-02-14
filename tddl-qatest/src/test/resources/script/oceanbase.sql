CREATE TABLE ob_normaltbl_onegroup_oneatom (
  pk bigint NOT NULL,
  id int DEFAULT NULL,
  gmt_create datetime DEFAULT NULL,
  gmt_timestamp timestamp NULL DEFAULT NULL,
  gmt_datetime datetime DEFAULT NULL,
  name varchar(20) DEFAULT NULL,
  floatCol float DEFAULT '0.000',
  PRIMARY KEY (pk)
)

CREATE TABLE ob_student_onegroup_oneatom (
  ID bigint NOT NULL,
  name varchar(255) DEFAULT NULL,
  school varchar(255) DEFAULT NULL,
  PRIMARY KEY (ID)
)

CREATE TABLE ob_module_info_onegroup_oneatom (
  module_id bigint NOT NULL DEFAULT '0',
  product_id bigint DEFAULT NULL,
  module_name varchar(20) DEFAULT NULL,
  parent_module_id bigint DEFAULT NULL,
  applevel varchar(255) DEFAULT NULL,
  apprisk varchar(255) DEFAULT NULL,
  sequence bigint DEFAULT NULL,
  module_description varchar(255) DEFAULT NULL,
  alias_name varchar(255) DEFAULT NULL,
  PRIMARY KEY (module_id)
)

CREATE TABLE ob_module_host_onegroup_oneatom (
  id bigint NOT NULL,
  module_id bigint DEFAULT NULL,
  host_id bigint DEFAULT NULL,
  PRIMARY KEY (id)
)

CREATE TABLE ob_hostgroup_info_onegroup_oneatom (
  hostgroup_id bigint NOT NULL,
  station_id varchar(255) DEFAULT NULL,
  hostgroup_name varchar(255) DEFAULT NULL,
  module_id bigint DEFAULT NULL,
  nagios_id bigint DEFAULT NULL,
  hostgroup_flag bigint DEFAULT NULL,
  PRIMARY KEY (hostgroup_id)
)

CREATE TABLE ob_host_info_onegroup_oneatom (
  host_id bigint NOT NULL DEFAULT '0',
  host_name varchar(255) DEFAULT NULL,
  host_ip varchar(255) DEFAULT NULL,
  host_type_id varchar(255) DEFAULT NULL,
  hostgroup_id bigint DEFAULT NULL,
  station_id varchar(255) DEFAULT NULL,
  snmp_community varchar(255) DEFAULT NULL,
  status varchar(255) DEFAULT NULL,
  host_flag bigint DEFAULT NULL,
  PRIMARY KEY (host_id)
)