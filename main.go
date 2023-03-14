/*
#################################
Jira Task Id - TRB 1347
Manasa Sai K
#################################
*/

package main

/*import the required modules*/
import (
	"ConsentCreditsReconciliationPackages_V1/Aerospike"
	"ConsentCreditsReconciliationPackages_V1/ConfigModule"
	"ConsentCreditsReconciliationPackages_V1/SendMail"
	"ConsentCreditsReconciliationPackages_V1/mysqldb"
	"ConsentCreditsReconciliationPackages_V1/time_logging"
	"encoding/csv"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	//"math"
)

//struct definition of the csv row data
type CSVRowData struct {
	EntityID  int
	TotalDB   int
	TotalAQL  int
	TotalDIFF int
	BlockDB   int
	BlockAQL  int
	BlockDIFF int
	UsedDB    int
	UsedAQL   int
	UsedDIFF  int
	PackID    int
}

/*Global variables*/
var (
	_, filePath, _, _        = runtime.Caller(0)
	present_working_filename = "[CNST_CRED_RECON ==> " + filepath.Base(filePath) + " ]"
	App_interrupt_flag       = false
	config_obj               = &ConfigModule.Configurations{}
	Log_obj                  *time_logging.Custom_log
	mysql_obj                mysqldb.Mysql_obj
	aero_obj                 Aerospike.Aerospike_type
	sigs_app                 = make(chan os.Signal, 1)
	LoadEnterpriseMap        = make(map[string][]int)
	Sendmail_obj             = &SendMail.SendMail_Type{}
	AppWaitGroups            sync.WaitGroup
	AppClose                 bool
	Proccess_done_flag       bool
	CsvData_slice            []CSVRowData
	Primary_keys             []string
)

/*WORKING
This is an error check function. It checks if an err type variable is nil or not.
Incase of not nil, it prints the error FileName,LineNo,Error description
ARGUMENTS - err variable of the type error
RETURN TYPE - bool [ true => if the error occured , false => if there is no error]
*/
func Check_Error(err error) bool {
	if err != nil {
		//fmt.Println(err)
		_, _, line, _ := runtime.Caller(1)
		Log_obj.Log("error", "--> ERROR IN LINE "+strconv.Itoa(line)+" ["+fmt.Sprint(err.Error())+"]")
		return true
	} else {
		return false
	}
}

/*WORKING 	- Load the config parameters required for the ConsentsCreditsReconciliation
ARGUMENTS	-   config_file , which is a json file holding the configuration parameters
RETURN TYPE - bool [true => if the function is implemented successfully , false => if function failed]
*/
func Load_config_parameters(config_file string) bool {

	if config_obj.ConfigurationsLoading(config_file) {
		return true
	} else {
		fmt.Println(present_working_filename + " ConsentsCreditsReconciliation APP ConfigurationsLoading is Failed")
		return false
	}

}

/*WORKING 	 - Create the objects required for the ConsentsCreditsReconciliation
  ARGUMENTS  - no args
  RETURN TYPE - bool [true => if the function is implemented successfully , false => if function failed]
*/
func Create_objects() bool {
	mysqldb.Config_obj = config_obj
	Aerospike.Config_obj = config_obj
	Log_obj = &time_logging.Custom_log{Filename: config_obj.LoggerConfigDetails.LogFileName, Filepath: config_obj.LoggerConfigDetails.LogFilePath, Loglevel: config_obj.LoggerConfigDetails.LogLevel}
	mysqldb.Log_obj = &time_logging.Custom_log{Filename: config_obj.LoggerConfigDetails.LogFileName, Filepath: config_obj.LoggerConfigDetails.LogFilePath, Loglevel: config_obj.LoggerConfigDetails.LogLevel}
	Aerospike.Log_obj = &time_logging.Custom_log{Filename: config_obj.LoggerConfigDetails.LogFileName, Filepath: config_obj.LoggerConfigDetails.LogFilePath, Loglevel: config_obj.LoggerConfigDetails.LogLevel}
	mysql_obj.Load_series_config_parameters()
	aero_obj.Load_config_parameters()
	Log_obj.Appname = "[" + config_obj.LoggerConfigDetails.LogPrefix + " ==> " + filepath.Base(filePath) + " ]"
	Sendmail_obj = &SendMail.SendMail_Type{Host: config_obj.SMTPServerConfigDetails.SMTPServer, Port: config_obj.SMTPServerConfigDetails.SMTPPort, Username: config_obj.SMTPServerConfigDetails.SMTPUserName, Password: config_obj.SMTPServerConfigDetails.SMTPPassword}
	SendMail.Log_obj = &time_logging.Custom_log{Filename: config_obj.LoggerConfigDetails.LogFileName, Filepath: config_obj.LoggerConfigDetails.LogFilePath, Loglevel: config_obj.LoggerConfigDetails.LogLevel}
	SendMail.Log_obj.Appname = SendMail.Present_working_filename
	return true

}

/*WORKING	- Create the connections required for the ConsentsCreditsReconciliation
  ARGUMENTS  - no args
RETURN TYPE - bool [true => if the function is implemented successfully , false => if function failed]
*/
func Create_connections() bool {
	if mysql_obj.Mysql_Connect() {

		if aero_obj.Aerospike_connect() {

			Log_obj.Log("info", "SUCCESSFULLY CREATED CONNECTIONS")
			return true

		}
	}

	return false
}

//Gets the index position of an element in a slice
func GetIndex(search_element string, search_slice []string) int {

	for ind, v := range search_slice {
		if v == search_element {
			return ind
		}
	}
	return -1
}

//===================================BATCH PROCESSING TO UPDATE DB/AQL DISCREPANCIES=====================================================

/*
WORKING-Batch_Processing_ExpiredCredits is used to perform this whole Loading + reconciliation batch wise for expired credits . istatus=2--> mysql batch fetch & Aerospike Batch Get
ARGUMENTS  - no args
RETURN TYPE - bool [true => if the function is implemented successfully , false => if function failed]
*/
func Batch_Processing_ExpiredCredits() bool {

	Log_obj.Log("info", "STARTING  PROCESSING FOR EXPIRED CREDITS")

	if Loading_Expired_Enterprise_Credits() {
		if Reconciliating_Expired_Enterprise_Credits() {

			//clearing LoadEnterpriseMap- before starting a new batch
			for k := range LoadEnterpriseMap {
				delete(LoadEnterpriseMap, k)
			}
			Primary_keys = nil

		} else {
			Log_obj.Log("error", "Reconciliating_Enterprise_Credits() FAILED")
			return false
		}
	} else {
		Log_obj.Log("error", "Loading_Enterprise_Credits() FAILED")
		return false

	}

	return true

}

/*
WORKING - This function Loads the data into LoadEnterpriseMap from the MYSQL table name in batch wise
ARGUMENTS - limit int, offset int
RETURN TYPE - bool [true => if the function is implemented successfully , false => if function failed]
*/
func Loading_Expired_Enterprise_Credits() bool {

	ok := mysql_obj.LoadExpiredEnterpriseCreditsRecords()
	if !ok {
		Log_obj.Log("error", "[Mysql Load failed]")
		return false
	}

	//populate the LoadEnterpriseMap from the mysql table data
	for mysql_obj.QueryResultRows.Next() {

		if App_interrupt_flag {
			return true
		}

		var iID, iEntityID, iTotal, iBlocked, iUsed int
		mysql_obj.QueryResultRows.Scan(&iID, &iEntityID, &iTotal, &iBlocked, &iUsed)
		combined_pk := fmt.Sprintf("%v-%v", iEntityID, iID)
		LoadEnterpriseMap[combined_pk] = []int{iTotal, iBlocked, iUsed}
		Primary_keys = append(Primary_keys, combined_pk)

	}
	Log_obj.Log("info", "[Total fetched mysql Expired records : "+fmt.Sprintf("%v]", len(LoadEnterpriseMap)))

	return true
}

/*
WORKING-Reconciliating_Expired_Enterprise_Credits Performs the below functionalities
1. BatchGet for the Primarykeys slice[] from the Aerospike
2. Compare Each of the corresponsing PK (entityID-Pack) in DB and AQL 's Credits value ==> Total , used , blocked
3. If Any discrepancies are found :
	a. Corresponding pk found in DB not found in AQL ==> Log the entry
	b. Mismatch in Total or Used or Blocked ==> Log the entry + Update the AQL data into Mysql table

*/
func Reconciliating_Expired_Enterprise_Credits() bool {

	ok, Records_slice := aero_obj.Aerospike_BatchRead(Primary_keys)
	msg := fmt.Sprintf("Reconciliating with Aerospike - Namespace : %v , SetName : %v", config_obj.AeroSpikeConfigDetails.AeroSpikeNameSpace, config_obj.AeroSpikeConfigDetails.AeroSpikeSetName)
	Log_obj.Log("info", msg)
	if !ok {
		return false
	}

	var index int
	for combined_pk, credits_slice := range LoadEnterpriseMap {

		var TotalAQL, BlockAQL, UsedAQL int
		index = GetIndex(combined_pk, Primary_keys)

		//PK - Enterprise missing in Aerospike
		if Records_slice[index] == nil {
			fmt.Println()
			body := fmt.Sprintf("Discrepancy : EntityID-iID %v Not found in Aerospike.", combined_pk)
			msg1 := fmt.Sprintf("DB_CREDITS ==>[TOTAL : %v,USED : %v,BLOCKED :%v]", credits_slice[0], credits_slice[2], credits_slice[1])
			Log_obj.Log("warning", body)
			Log_obj.Log("info", msg1)
			fmt.Println()


		} else {

			//Record found in Aerospike , Checking for differences
			TotalAQL = Records_slice[index].Bins["Total"].(int)
			BlockAQL = Records_slice[index].Bins["Blocked"].(int)
			UsedAQL = Records_slice[index].Bins["Used"].(int)
			//TotalAQL = TotalAQL + 1 //testing

			if TotalAQL != credits_slice[0] || BlockAQL != credits_slice[1] || UsedAQL != credits_slice[2] {
				fmt.Println()
				Log_obj.Log("warning", "Discrepancy : Below Mismatched Credits between DB and AQL for Entity id :"+combined_pk)
				msg1 := fmt.Sprintf("DB_CREDITS ==>[TOTAL : %v,USED : %v,BLOCKED :%v]", credits_slice[0], credits_slice[2], credits_slice[1])
				msg2 := fmt.Sprintf("AQL_CREDITS ==>[TOTAL : %v,USED : %v,BLOCKED :%v]", TotalAQL, UsedAQL, BlockAQL)
				Log_obj.Log("info", msg1)
				Log_obj.Log("info", msg2)
				fmt.Println()
				mysql_obj.Update_EnterpriseCredits_Records(strings.Split(combined_pk, "-")[1], []int{TotalAQL, BlockAQL, UsedAQL})

			}

		}

	}
	return true

}

//========================================BATCH PROCESSING FOR CREDITS PACK STATS CSV REPORT=========================================================
/*
WORKING- Batch_Processing_for_Report is used to perform this whole Loading + reconciliation batch wise --> mysql batch fetch & Aerospike Batch Get
ARGUMENTS  - no args
RETURN TYPE - bool [true => if the function is implemented successfully , false => if function failed]
*/
func Batch_Processing_for_Report() bool {

	Log_obj.Log("info", "STARTING BATCH PROCESSING FOR CREDITS PACKS STATS")

	if mysql_obj.CountOfRecords() {

		var offset, remainder, i int

		//Total Batches = CountofRecords/BatchSize [for ex: 10 = 105/10 , remainder = 5]
		//Fetching the records batch-wise
		TotalBatchCount := mysqldb.RecordCount / config_obj.MySQLConfigDetails.MySQLBatchSize
		remainder = mysqldb.RecordCount % config_obj.MySQLConfigDetails.MySQLBatchSize
		for i = 0; i < TotalBatchCount; i++ {

			//checking interrupt before processing the current batch
			if App_interrupt_flag {
				return true
			}

			offset = config_obj.MySQLConfigDetails.MySQLBatchSize * i

			if Loading_Enterprise_Credits(config_obj.MySQLConfigDetails.MySQLBatchSize, offset) {
				if Reconciliating_Enterprise_Credits() {

					//clearing LoadEnterpriseMap- before starting a new batch
					for k := range LoadEnterpriseMap {
						delete(LoadEnterpriseMap, k)
					}
					Primary_keys = nil

				} else {
					Log_obj.Log("error", "Reconciliating_Enterprise_Credits() FAILED")
					return false
				}
			} else {
				Log_obj.Log("error", "Loading_Enterprise_Credits() FAILED")
				return false

			}

			Log_obj.Log("info", "=======Processed Batch======== : "+fmt.Sprintf("%v", i))

		}

		//if remainder records exists...batch process them
		if remainder != 0 {

			offset = config_obj.MySQLConfigDetails.MySQLBatchSize * i

			if Loading_Enterprise_Credits(config_obj.MySQLConfigDetails.MySQLBatchSize, offset) {
				if Reconciliating_Enterprise_Credits() {
					//clearing LoadEnterpriseMap- before starting a next BatchProcessing
					for k := range LoadEnterpriseMap {
						delete(LoadEnterpriseMap, k)
					}
					Primary_keys = nil
					Log_obj.Log("info", "=======Last Processed Batch======== : "+fmt.Sprintf("%v", i))

				} else {
					Log_obj.Log("error", "Reconciliating_Enterprise_Credits() FAILED")
					return false
				}
			} else {
				Log_obj.Log("error", "Loading_Enterprise_Credits() FAILED")
				return false

			}
		}

		return true

	}
	Log_obj.Log("error", "CountOfRecords() FAILED")
	return false

}

/*
WORKING - This function Loads the data into LoadEnterpriseMap from the MYSQL table name batch wise
ARGUMENTS - limit int, offset int
RETURN TYPE - bool [true => if the function is implemented successfully , false => if function failed]
*/
func Loading_Enterprise_Credits(limit int, offset int) bool {

	ok := mysql_obj.Load_EnterpriseCredits_Records(limit, offset)
	if !ok {
		Log_obj.Log("error", "[Mysql Load failed]")
		return false
	}

	//populate the LoadEnterpriseMap from the mysql table data
	for mysql_obj.QueryResultRows.Next() {

		if App_interrupt_flag {
			return true
		}

		var iID, iEntityID, iTotal, iBlocked, iUsed int
		mysql_obj.QueryResultRows.Scan(&iID, &iEntityID, &iTotal, &iBlocked, &iUsed)
		combined_pk := fmt.Sprintf("%v-%v", iEntityID, iID)
		LoadEnterpriseMap[combined_pk] = []int{iTotal, iBlocked, iUsed}
		Primary_keys = append(Primary_keys, combined_pk)

	}
	Log_obj.Log("info", "[Total fetched mysql records in the present batch : "+fmt.Sprintf("%v]", len(LoadEnterpriseMap)))

	return true
}

/*
WORKING-Reconciliating_Enterprise_Credits Performs the below functionalities
1. BatchGet for the Primarykeys slice[] from the Aerospike
2. Compare Each of the corresponsing PK (entityID-Pack) in DB and AQL 's Credits value ==> Total , used , blocked
3. If Any discrepancies are found :
	a. Corresponding pk found in DB not found in AQL ==> Log the entry
	b. Mismatch in Total or Used or Blocked ==> Log the entry
4. consolidated data of stats of enterprise pack and credits for DB and AQL

*/
func Reconciliating_Enterprise_Credits() bool {

	ok, Records_slice := aero_obj.Aerospike_BatchRead(Primary_keys)

	msg := fmt.Sprintf("Reconciliating with Aerospike - Namespace : %v , SetName : %v", config_obj.AeroSpikeConfigDetails.AeroSpikeNameSpace, config_obj.AeroSpikeConfigDetails.AeroSpikeSetName)
	Log_obj.Log("info", msg)
	if !ok {
		return false
	}

	var index int
	for combined_pk, credits_slice := range LoadEnterpriseMap {

		var TotalAQL, BlockAQL, UsedAQL int
		html_record := CSVRowData{}
		index = GetIndex(combined_pk, Primary_keys)

		//PK - Enterprise missing in Aerospike
		if Records_slice[index] == nil {
			fmt.Println()
			body := fmt.Sprintf("Discrepancy : EntityID %v Not found in Aerospike.", combined_pk)
			msg1 := fmt.Sprintf("DB_CREDITS ==>[TOTAL : %v,USED : %v,BLOCKED :%v]", credits_slice[0], credits_slice[2], credits_slice[1])
			Log_obj.Log("warning", body)
			Log_obj.Log("info", msg1)
			fmt.Println()


		} else {

			//Record found in Aerospike , Checking for differences
			TotalAQL = Records_slice[index].Bins["Total"].(int)
			BlockAQL = Records_slice[index].Bins["Blocked"].(int)
			UsedAQL = Records_slice[index].Bins["Used"].(int)
			//TotalAQL = TotalAQL + 1 //testing

			if TotalAQL != credits_slice[0] || BlockAQL != credits_slice[1] || UsedAQL != credits_slice[2] {
				fmt.Println()
				Log_obj.Log("warning", "Discrepancy : Below Mismatched Credits between DB and AQL for Entity id :"+combined_pk)
				msg1 := fmt.Sprintf("DB_CREDITS ==>[TOTAL : %v,USED : %v,BLOCKED :%v]", credits_slice[0], credits_slice[2], credits_slice[1])
				msg2 := fmt.Sprintf("AQL_CREDITS ==>[TOTAL : %v,USED : %v,BLOCKED :%v]", TotalAQL, UsedAQL, BlockAQL)
				Log_obj.Log("info", msg1)
				Log_obj.Log("info", msg2)
				fmt.Println()

			}
		}

		//Preparing the Discrepancy Table Data
		entityId, _ := strconv.Atoi(strings.Split(combined_pk, "-")[0])
		PackID, _ := strconv.Atoi(strings.Split(combined_pk, "-")[1])

		html_record.EntityID = entityId
		html_record.PackID = PackID
		html_record.TotalDB = credits_slice[0]
		html_record.TotalAQL = TotalAQL
		html_record.TotalDIFF = credits_slice[0] - TotalAQL
		html_record.BlockDB = credits_slice[1]
		html_record.BlockAQL = BlockAQL
		html_record.BlockDIFF = credits_slice[1] - BlockAQL
		html_record.UsedDB = credits_slice[2]
		html_record.UsedAQL = UsedAQL
		html_record.UsedDIFF = credits_slice[2] - UsedAQL
		CsvData_slice = append(CsvData_slice, html_record)

		//fmt.Println(iEntityID)

	}
	//fmt.Println(len(CsvData_slice))
	return true

}

/*WORKING- GenerateReport , prepares the rowwise data which needs to be updated in the csv report */
func GenerateReport(CsvData_slice []CSVRowData, filename string) bool {

	//creating file
	file, err := os.Create(filepath.Join(config_obj.LoggerConfigDetails.LogFilePath, filename))
	defer file.Close()
	if Check_Error(err) {
		return false
	}
	w := csv.NewWriter(file)
	defer w.Flush()

	//adding the header
	header := []string{"S.No", "EntityID", "PackID", "TotalDB", "TotalAQL", "TotalDIFF", "BlockDB", "BlockAQL", "BlockDIFF", "UsedDB", "UsedAQL", "UsedDIFF"}
	if err := w.Write(header); Check_Error(err) {
		Log_obj.Log("error", "error writing header to csv file")
		return false
	}

	//generating report
	for index, row_data := range CsvData_slice {
		row := []string{fmt.Sprint(index + 1), fmt.Sprint(row_data.EntityID) + "\t", fmt.Sprint(row_data.PackID), fmt.Sprint(row_data.TotalDB), fmt.Sprint(row_data.TotalAQL), fmt.Sprint(row_data.TotalDIFF), fmt.Sprint(row_data.BlockDB), fmt.Sprint(row_data.BlockAQL), fmt.Sprint(row_data.BlockDIFF), fmt.Sprint(row_data.UsedDB), fmt.Sprint(row_data.UsedAQL), fmt.Sprint(row_data.UsedDIFF)}
		if err := w.Write(row); Check_Error(err) {
			Log_obj.Log("error", "error writing record to file")
			return false
		}
	}

	return true

}

/*WORKING - PrepareCSVReport_and_Mail is used to send the mail embedded with the csv report as the attachment.
The report name is concatenated with current timestamp*/
func PrepareCSVReport_and_Mail() bool {

	if len(CsvData_slice) > 0 {

		//CSV FILE GENERATION WITH CURRENT TIMESTAMP
		t := time.Now()
		y := strconv.Itoa(t.Year())
		mon := fmt.Sprintf("%02d", int(t.Month()))
		d := fmt.Sprintf("%02d", int(t.Day()))
		present_hour := fmt.Sprintf("%02d", int(t.Hour()))
		filename_format := fmt.Sprintf(config_obj.SMTPServerConfigDetails.Report_FileName + "_" + y + "-" + mon + "-" + d + "_" + present_hour + ".csv")

		if GenerateReport(CsvData_slice, filename_format) {

			//SEND MAIL 
			Sendmail_obj.SendMail(config_obj.SMTPServerConfigDetails.MailFromAddr, config_obj.SMTPServerConfigDetails.MailToAddr, config_obj.SMTPServerConfigDetails.Subject, config_obj.LoggerConfigDetails.LogFilePath+filename_format)
			

		} else {
			Log_obj.Log("error", "GenerateReport() Failed")
		}

	}

	return true
}

/*WORKING	- This function closes all the mysql and aerospike connections
ARGUMENTS   - no args
RETURN TYPE - bool
*/
func Close_connections() bool {

	if aero_obj.Close_connections() {
		if mysql_obj.Close_connections() {

			Log_obj.Log("info", "[closed all  connections]")
			return true

		}

	}

	Log_obj.Log("error", "[Error closing  connections]")
	return false
}

/*

================================MAIN FUNCTION===================================================
This Application runs as the below:
I. Load the config parameters and Create the objects/connections required for the APP to run.

II. Checks for discrepancies in credits for a particular (Enterprise Id-packid) , in DB and AQL
	if found ==> Log them...
	Finally send mail with a csv file attachment comprising of stats of all the enterprise packs and credits
	for both DB and AQL

III. Close the connections when the APP stops.
================================================================================================
*/

func init() {
	signal.Notify(sigs_app, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs_app
		mysql_obj.Mysql_interrupt_flag = true
		Aerospike.Aero_interrupt_flag = true
		App_interrupt_flag = true
		Log_obj.Log("warning", "[ USER PRESSED CTRL-C. APPLICATION WILL EXIT AFTER PROCESSING EXISTING RECORDS.]")
		return
	}()
}

func main() {

	defer func() {
		if err := recover(); err != nil {
			fmt.Println("panic occurred in CNST CRED EMAIL==>main.go ", err)
			os.Exit(0)
		}

	}()

	if len(os.Args) != 2 {
		fmt.Println("PROVIDE COMMAND LINE ARGUMENT")
		fmt.Println("RUN COMMAND : go run . CNST_CRED_RECON.json ")
	} else {
		if strings.ToUpper(os.Args[1]) == "-V" || strings.ToUpper(os.Args[1]) == "--VERSION" {
			fmt.Println("CNST_CRED_RECON 1.0.0 ")
			os.Exit(0)
		}

		config_file := os.Args[1]
		// Checks if the Load_config_parameters() is successful for CNST CRED EMAIL App
		if Load_config_parameters(config_file) {

			// Checks if the Create_objects() is successful for CNST CRED EMAIL App
			if Create_objects() {

				// Checks if the Create_connections() is successful for CNST CRED EMAIL App
				if Create_connections() {

					//1. CSV REPORT
					if Batch_Processing_for_Report() {

						//SEND MAIL IF ONLY FLAG IS ENABLED
						if config_obj.SMTPServerConfigDetails.IsMailToBeSent {

							if PrepareCSVReport_and_Mail() {

							} else {
								Log_obj.Log("error", "PrepareCSVReport_and_Mail Failed")
							}

							Log_obj.Log("info", "isMailtoBeSent Flag enabled. Report Generated + Mail Sent")

						}

						Log_obj.Log("info", "FINISHED BATCH PROCESSING FOR CREDITS PACKS STATS")

					} else {
						Log_obj.Log("error", "[Batch_Processing_for_Report FAILED]")

					}

					//2.UPDATE ANY DISCREPANCIES
					if Batch_Processing_ExpiredCredits() {
						Log_obj.Log("info", "FINISHED BATCH PROCESSING FOR EXPIRED CREDITS")

					} else {
						Log_obj.Log("error", "[Batch_Processing_ExpiredCredits FAILED]")

					}

				} else {
					Log_obj.Log("error", "[ConsentsCreditsReconciliation APP Create_connections FAILED]")

				}
			} else {
				fmt.Println("ConsentsCreditsReconciliation APP Create_objects FAILED")

			}
		} else {
			fmt.Println("ConsentsCreditsReconciliation APP Load_config_parameters FAILED")

		}

		Close_connections()
		os.Exit(0)
	}

}
