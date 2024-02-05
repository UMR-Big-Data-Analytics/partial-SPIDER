package runner;

import java.io.File;

public class Config {

    final double threshold;
    public Config.Algorithm algorithm;
    public String databaseName;
    public String[] tableNames;
    public String inputFolderPath = "M:\\MA\\data" + File.separator;
    public String inputFileEnding = ".csv";
    public char inputFileSeparator = ',';
    public char inputFileQuoteChar = '\"';
    public char inputFileEscape = '\\';
    public int inputFileSkipLines = 0;
    public boolean inputFileStrictQuotes = true;
    public boolean inputFileIgnoreLeadingWhiteSpace = true;
    public boolean inputFileHasHeader = true;
    public boolean inputFileSkipDifferingLines = true; // Skip lines that differ from the dataset's schema
    public String inputFileNullString = "";
    public String statisticsFileName = "IND_statistics.txt";
    public String resultFileName = "IND_results.txt";
    public boolean writeResults = true;


    public Config(Config.Algorithm algorithm, Config.Dataset dataset, double threshold) {
        this.algorithm = algorithm;
        this.setDataset(dataset);
        this.threshold = threshold;
    }

    private void setDataset(Config.Dataset dataset) {
        switch (dataset) {
            case KAGGLE -> {
                this.databaseName = "Kaggle\\";
                this.tableNames = new String[]{"enrollement_schoolmanagement_2", "data", "amazon_laptop_prices_v01", "IQ_level", "Employee", "employee_data (1)"};
                this.inputFileSeparator = ',';
                this.inputFileHasHeader = true;
            }
            case TPCH_1 -> {
                this.databaseName = "TPCH_1\\";
                this.tableNames = new String[]{"customer", "lineitem", "nation", "orders", "part", "region", "supplier"};
                this.inputFileSeparator = '|';
                this.inputFileHasHeader = false;
                this.inputFileEnding = ".tbl";
            }
            case DATA_GOV -> {
                this.databaseName = "data.gov\\";
                this.tableNames = new String[]{"Air_Quality", "Air_Traffic_Passenger_Statistics",
                        "Crash_Reporting_-_Drivers_Data", "Crime_Data_from_2020_to_Present", "Demographic_Statistics_By_Zip_Code",
                        "diabetes_all_2016", "Electric_Vehicle_Population_Data", "iou_zipcodes_2020",
                        "Lottery_Mega_Millions_Winning_Numbers__Beginning_2002", "Lottery_Powerball_Winning_Numbers__Beginning_2010",
                        "Motor_Vehicle_Collisions_-_Crashes", "National_Obesity_By_State",
                        "NCHS_-_Death_rates_and_life_expectancy_at_birth", "Popular_Baby_Names", "Real_Estate_Sales_2001-2020_GL",
                        "Traffic_Crashes_-_Crashes", "Warehouse_and_Retail_Sales"
                };
                this.inputFileSeparator = ',';
                this.inputFileHasHeader = true;
                this.inputFileEnding = ".csv";
            }
            case UEFA -> {
                this.databaseName = "uefa\\";
                this.tableNames = new String[]{"attacking", "attempts", "defending", "disciplinary", "distributon",
                        "goalkeeping", "goals", "key_stats"
                };
                this.inputFileSeparator = ',';
                this.inputFileHasHeader = true;
                this.inputFileEnding = ".csv";
            }
            default -> {
            }
        }
    }

    public enum Algorithm {
        SPIDER
    }

    public enum Dataset {
        TPCH_1, KAGGLE, DATA_GOV, UEFA
    }
}