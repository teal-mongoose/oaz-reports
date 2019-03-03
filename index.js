var datefns = require ('date-fns')

const projectId = "oaz-reports"
const datasetId = "oaz_dataset"
const filename = "./exports/pbs.json"
const tableId = "oaz_pbs"

loadLocalFile(datasetId, tableId, filename, projectId)

function loadLocalFile(datasetId, tableId, filename, projectId) {
  let cnt = 0;
  const { BigQuery } = require('@google-cloud/bigquery');
  const bigquery = new BigQuery({ projectId });
  data = require(filename)

  for (const line of data) {
    cnt++
    line.Last_Insur_Pmnt_Date = line.Last_Insur_Pmnt_Date ? datefns.format(new Date(line.Last_Insur_Pmnt_Date), 'YYYY-MM-DD') : null
    line.Last_Payment_Date = line.Last_Payment_Date ? datefns.format(new Date(line.Last_Payment_Date), 'YYYY-MM-DD') : null
    line.Last_Pt_Pmnt_Date = line.Last_Pt_Pmnt_Date ? datefns.format(new Date(line.Last_Pt_Pmnt_Date), 'YYYY-MM-DD') : null
    line.Last_Service_Date = line.Last_Service_Date ? datefns.format(new Date(line.Last_Service_Date), 'YYYY-MM-DD') : null
    bigquery
      .dataset(datasetId)
      .table(tableId)
      .insert(line)
      .then()
      .catch(e => {
        e.errors.forEach(x => {
          console.log(x.errors) 
        })
      })
    }
  console.log(`Finished, wrote ${cnt} lines`)
}

