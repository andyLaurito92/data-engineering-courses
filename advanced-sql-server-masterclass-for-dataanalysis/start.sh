docker run -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=<YourStrong@Passw0rd>" \
   -p 1433:1433 --name sql1 --hostname sql1 \
   -d \
   -v /Users/andylaurito/Desktop/data-engineering/advanced-sql-server-masterclass-for-dataanalysis:/var/opt/mssql/backup \
   mcr.microsoft.com/mssql/server:2022-latest

docker exec -it sql1 /opt/mssql-tools/bin/sqlcmd -S localhost \
   -U SA -P '<YourStrong@Passw0rd>' \
   -Q 'RESTORE FILELISTONLY FROM DISK = "/var/opt/mssql/backup/AdventureWorks2019.bak"' \
   | tr -s ' ' | cut -d ' ' -f 1-2

docker exec -it sql1 /opt/mssql-tools/bin/sqlcmd \
   -S localhost -U SA -P '<YourStrong@Passw0rd>' \
   -Q 'RESTORE DATABASE AdventureWorks2017 FROM DISK = "/var/opt/mssql/backup/AdventureWorks2019.bak" WITH MOVE "AdventureWorks2017" TO "/var/opt/mssql/data/AdventureWorks2019.mdf", MOVE "AdventureWorks2017_log" TO "/var/opt/mssql/data/AdventureWorks2019_log.mdf"' 

docker exec -it sql1 /opt/mssql-tools/bin/sqlcmd \
   -S localhost -U SA -P '<YourStrong@Passw0rd>' \
   -Q 'SELECT * FROM INFORMATION_SCHEMA.TABLES; GO'
