package ie.ibuttimer.weather;

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;

public class Constants {

    public static final String COMMENT_PREFIX = "#";

    public static final String FAMILY = "cf";
    public static final byte[] FAMILY_BYTES = FAMILY.getBytes();

    /*
        hbase(main):004:0> get "weather_info", "r-2020063015"
        COLUMN                               CELL
         cf:date                             timestamp=1596534897470, value=2020-06-30 15:00:00
         cf:dewpt_3904                       timestamp=1596534897470, value=13.7
         cf:ind_rain_3904                    timestamp=1596534897470, value=0
         cf:ind_temp_3904                    timestamp=1596534897470, value=0
         cf:ind_wetb_3904                    timestamp=1596534897470, value=0
         cf:msl_3904                         timestamp=1596534897470, value=1002.3
         cf:rain_3904                        timestamp=1596534897470, value=0.0
         cf:rhum_3904                        timestamp=1596534897470, value=86
         cf:temp_3904                        timestamp=1596534897470, value=16.0
         cf:vappr_3904                       timestamp=1596534897470, value=15.7
         cf:wetb_3904                        timestamp=1596534897470, value=14.7
     */
    public static final String DATE_COL = "date";
    public static final byte[] DATE_ATTR = DATE_COL.getBytes();

    public static final String DFLT_DATETIME_FMT = "yyyy-MM-dd HH:mm:ss";
    public static final DateTimeFormatter DATETIME_FMT = new DateTimeFormatterBuilder().
            appendPattern(DFLT_DATETIME_FMT).toFormatter();
    public static final DateTimeFormatter YYYYMMDDHH_FMT = new DateTimeFormatterBuilder().
            appendPattern("yyyyMMddHH").toFormatter();

    public static final String REDUCER_STATS = "reducer.stats";

    public static final String DEWPT_COL = "dewpt";
    public static final String IND_RAIN_COL = "ind_rain";
    public static final String IND_TEMP_COL = "ind_temp";
    public static final String IND_WETB_COL = "ind_wetb";
    public static final String MSL_COL = "msl";
    public static final String RAIN_COL = "rain";
    public static final String RHUM_COL = "rhum";
    public static final String TEMP_COL = "temp";
    public static final String VAPPR_COL = "vappr";
    public static final String WETB_COL = "wetb";

    public static String stationColumn(String column, int station) {
        return column + "_" + Integer.toString(station);
    }
    public static String stationDewptColumn(int station) {
        return stationColumn(DEWPT_COL, station);
    }
    public static String stationIndRainColumn(int station) {
        return stationColumn(IND_RAIN_COL, station);
    }
    public static String stationIndTempColumn(int station) {
        return stationColumn(IND_TEMP_COL, station);
    }
    public static String stationIndWetbColumn(int station) {
        return stationColumn(IND_WETB_COL, station);
    }
    public static String stationMslColumn(int station) {
        return stationColumn(MSL_COL, station);
    }
    public static String stationRainColumn(int station) {
        return stationColumn(RAIN_COL, station);
    }
    public static String stationRhumColumn(int station) {
        return stationColumn(RHUM_COL, station);
    }
    public static String stationTempColumn(int station) {
        return stationColumn(TEMP_COL, station);
    }
    public static String stationVapprColumn(int station) {
        return stationColumn(VAPPR_COL, station);
    }
    public static String stationWetbColumn(int station) {
        return stationColumn(WETB_COL, station);
    }
    public static byte[] stationDewptColumnBytes(int station) {
        return stationDewptColumn(station).getBytes();
    }
    public static byte[] stationIndRainColumnBytes(int station) {
        return stationIndRainColumn(station).getBytes();
    }
    public static byte[] stationIndTempColumnBytes(int station) {
        return stationIndTempColumn(station).getBytes();
    }
    public static byte[] stationIndWetbColumnBytes(int station) {
        return stationIndWetbColumn(station).getBytes();
    }
    public static byte[] stationMslColumnBytes(int station) {
        return stationMslColumn(station).getBytes();
    }
    public static byte[] stationRainColumnBytes(int station) {
        return stationRainColumn(station).getBytes();
    }
    public static byte[] stationRhumColumnBytes(int station) {
        return stationRhumColumn(station).getBytes();
    }
    public static byte[] stationTempColumnBytes(int station) {
        return stationTempColumn(station).getBytes();
    }
    public static byte[] stationVapprColumnBytes(int station) {
        return stationVapprColumn(station).getBytes();
    }
    public static byte[] stationWetbColumnBytes(int station) {
        return stationWetbColumn(station).getBytes();
    }




    public static final int STATUS_CONFIG_ERROR = -1;
    public static final int STATUS_SUCCESS = 0;
    public static final int STATUS_FAIL = 1;
    public static final int STATUS_RUNNING = 2;


    // config related
    public static final String DFLT_CFG_FILE = "config.properties";
    public static final String MULTIPLE_CFG_FILE_SEP = ";";

    public static final String CFG_HBASE_RESOURCE = "hbase_resource";
    public static final String DFLT_HBASE_RESOURCE = "hbase-site.xml";

    public static final String CFG_NUM_REDUCERS = "num_reducers";
    public static final int DFLT_NUM_REDUCERS = 1;

    public static final String CFG_SCAN_CACHING = "scan_caching";
    public static final int DFLT_SCAN_CACHING = 500;

    public static final String CFG_ANALYSIS_IN_TABLE = "analysis_in_table";
    public static final String CFG_ANALYSIS_OUT_TABLE = "analysis_out_table";
    public static final String CFG_TRANSFORM_IN_TABLE = "transform_in_table";
    public static final String CFG_TRANSFORM_STATS_TABLE = "transform_stats_table";
    public static final String CFG_TRANSFORM_OUT_TABLE = "transform_out_table";
    public static final String CFG_DIFFERENCING_IN_TABLE = "differencing_in_table";
    public static final String CFG_DIFFERENCING_OUT_TABLE = "differencing_out_table";

    public static final String CFG_SMA_IN_TABLE = "sma_in_table";

    public static final String CFG_KEY_TYPE_MAP = "key_type_map";

    public static final String CFG_IN_PATH_ROOT = "global.in_path_root";
    public static final String CFG_OUT_PATH_ROOT = "global.out_path_root";

    public static final String CFG_MODE = "mode";                           // run mode; 'dev' or 'run'
    public static final String CFG_CLR_LAST_RESULT = "clear_last_result";   // clear last result; ignored in run mode

    public static final String CFG_COLUMN_LIST = "column_list";          // required column list
    public static final String CFG_COLUMN_LIST_SEP = ",";


    public static final String CFG_MA_WINDOW_SIZE = "moving_average_window_size";   // sma window size
    public static final int DFLT_MA_WINDOW_SIZE = 3;                                // default sma window size
    public static final String CFG_DATETIME_FMT = "datetime_format";                // sma window size
    public static final String CFG_SMA_REDUCE_MODE = "sma_reduce_mode";             // sma reducer mode
    public static final String SMA_FILE_REDUCE_MODE = "file";
    public static final String SMA_TABLE_REDUCE_MODE = "table";
    public static final String DFLT_SMA_REDUCE_MODE = SMA_FILE_REDUCE_MODE;
    public static final String CFG_SMA_REDUCE_TABLE = "sma_reduce_table";           // table to store sma output
    public static final String DFLT_SMA_REDUCE_TABLE = "sma_info";

    public static final String CFG_TRANSFORM_LAG = "transform_lag";           // lag in hours
    public static final String CFG_TRANSFORM_DIFFERENCING = "transform_differencing";   // differencing in num of readings
    public static final String CFG_START_DATETIME = "start_datetime";          // filter start date/time
    public static final String CFG_STOP_DATETIME = "stop_datetime";           // filter end date/time

    public static final String CFG_NUM_STRATA = "num_strata";                   // number of strata
    public static final int DFLT_NUM_STRATA = 1;
    public static final String CFG_STRATA_WIDTH = "strata_width";                   // width of each strata
    public static final int DFLT_STRATA_WIDTH = 1;



    private Constants() {
        // can't instantiate class
    }
}
