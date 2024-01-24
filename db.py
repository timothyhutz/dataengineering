import mysql.connector
import logging, requests, zipfile, csv, time
from io import BytesIO
logging.basicConfig(level=logging.DEBUG)
## Pool setup
mysql.connector.pooling.MySQLConnectionPool(pool_size=10, pool_name='default')
try:
    cnx = mysql.connector.connect(pool_name='default', user='root', autocommit=True)
    cnx.cursor().execute('CREATE DATABASE main;')
    logging.info("DB main created")
    with zipfile.ZipFile(BytesIO(requests.get('https://simplemaps.com/static/data/us-zips/1.83/basic/simplemaps_uszips_basicv1.83.zip', stream=True).content), 'r',) as zip_data:
        zip_data.extract('uszips.csv')
    with open('uszips.csv', 'r') as file:
        table_statement = "".join(f"`{field}` varchar(255) NOT NULL," for field in csv.DictReader(file).fieldnames)
        table_statement =  "USE main; CREATE TABLE `zipcode_data` (" + table_statement + " PRIMARY KEY (`zip`) );"
        logging.debug(table_statement)
        try:
            cnx = mysql.connector.connect(pool_name='default', user='root', autocommit=True)
            ## Inital DB table create.
            cnx.cursor().execute(table_statement, multi=True)
            logging.debug("DB Table created")
            columns = [line for line in csv.reader(file)]
            sql_ready = list()
            for i in columns:
                i[i.index('')] = 'NULL'
                sql_ready.append(tuple(item for item in i))
            logging.debug(sql_ready[0:5])
            ## adding zipcode data to table
            logging.debug(f"{len(sql_ready[0])} columns detected for DB")
            cnx = mysql.connector.connect(pool_name='default', user='root', autocommit=True)
            statement = f"INSERT INTO zipcode_data VALUES ({ "%s, " * (len(sql_ready[0]) - 1) + "%s"});"
            cnx.cursor().execute('USE main;')
            cnx.cursor().executemany(statement, sql_ready)
            logging.info('Data loaded to zipcode table')
        except mysql.connector.Error as e:
            logging.warning(e)
            logging.warning('DB table creation setup failed and not complete')
except mysql.connector.Error as e:
    logging.warning(e)
    logging.warning('DB was not created from scratch')
