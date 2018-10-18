package elastic_search_test.config;

public class DatasourceConstant {
    public final static String ES_CLUSTER_NAME = "es-jw-darpa";
    public final static String ES_HOSTS = "172.16.150.149";
    public final static int ES_PORT = 29300;
    public final static String DATASOURCE_SETTING_INDEX = "event_*";
    public final static String DATASOURCE_SETTING_TYPE = "event";
    //    public final static String HAL_HOSTS = "172.16.150.149";
//    public final static int HAL_PORT = 8085;
    public final static String RULE_TEMPLATE_INDEX = "ueba_rule_template";
    public final static String RULE_TEMPLATE_TYPE = "rule_template";
    public final static String SCENARIO_SETTING_INDEX = "ueba_scenario_setting";
    public final static String SCENARIO_SETTING_TYPE = "scenario";
    public final static String LEFT = "left";
    public final static String RIGHT = "right";
    public final static String BLACKLIST_INDEX = "ueba_blacklist";
    public final static String BLACKLIST_TYPE = "blacklist";
    public final static String BLACKLIST_CATEGORY_INDEX = "ueba_blacklist_category";
    public final static String BLACKLIST_CATEGORY_TYPE = "blacklist_category";
    public final static String RULE_PATH = "rules.json";
    public final static String ENTITY_CONFIG_INDEX = "ueba_entity_config";
    public final static String ENTITY_CONFIG_TYPE = "entity_config";
    public final static String ENTITY_INDEX = "ueba_entity";
    public final static String ENTITY_TYPE = "entity";
    public final static String USER_INDEX = "ueba_user";
    public final static String USER_TYPE = "user_info";
    public final static String MLA_INDEX = "ueba_mla";
    public final static String MLA_TYPE = "mla";
    public final static String MLA_MODEL_INDEX = "ueba_mla_model";
    public final static String MLA_MODEL_TYPE = "model";
    public final static String TEST_PID_FILE = "pid/test_rule";
    public final static String PID_FILE = "pid/ueba-";
    //    public final static String KAFKA_SERVER = "172.16.150.149:9092";
//    public final static String KAFKA_GROUP = "ueba-event-process-";
    // rule test group id = KAFKA_GROUP_TEST + ruleId
//    public final static String KAFKA_GROUP_TEST = "ueba-event-process-test-";
    //public final static String KAFKA_TOPIC = "ueba-es-1";
//    public final static String KAFKA_TOPIC = "demo-3";
//    public final static String FLINK_BIN = "/opt/flink-1.4.2/bin/flink";
    public final static String TEST_RULE_INDEX = "ueba_alarm_test";
    public final static String ANOMALY_SCENARIOS = "anomaly_scenarios";
    public final static String ANOMALY_BEHAVIORS = "anomaly_behaviors";
    //    public final static String COLLECTOR_HOST = "172.16.150.149";
//    public final static String COLLECTOR_PORT = "9989";
    public final static String USER_CONFIG_INDEX = "ueba_user_config";
    public final static String USER_CONFIG_TYPE = "user_config";
    //    public final static String FLINK_TM_HOST = "127.0.0.1";
//    public final static Integer FLINK_STATE_SEVER_PROXY_PORT = 9069;
    public final static String STREAMING_DYNAMIC_PATH = "./";
    public final static String RULE_JAR = "rule.jar";
    public final static int SAMPLE_COUNT = 5;
    public final static int ES_BULK_SIZE = 500;

    public final static String UEBA_ALARM_INDEX = "ueba_alarm";

    public final static String UEBA_SETTINGS_INDEX = "ueba_settings";

}