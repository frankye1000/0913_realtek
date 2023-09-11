using ETLBox.ControlFlow;
using ETLBox.DataFlow;
using ETLBox.SqlServer;
using ETLBox.Csv;


namespace ETLBoxDemo.BasicExample
{
    class Program
    {
        static void Main(string[] args)
        {
            //Set up the connection manager to master
            var masterConnection = new SqlConnectionManager("Data Source=localhost;User Id=sa;Password=reallyStrongPwd123;TrustServerCertificate=true;");
            //Recreate database
            DropDatabaseTask.DropIfExists(masterConnection, "demo");
            CreateDatabaseTask.Create(masterConnection, "demo");

            //Get connection manager to previously create database
            var dbConnection = new SqlConnectionManager("Data Source=localhost;User Id=sa;Password=reallyStrongPwd123;Initial Catalog=demo;TrustServerCertificate=true;");

            //Create destination table
            CreateTableTask.Create(dbConnection, "Table1", new List<TableColumn>()
            {
                new TableColumn("ID","int",allowNulls:false, isPrimaryKey:true, isIdentity:true),
                new TableColumn("Col1","nvarchar(100)",allowNulls:true),
                new TableColumn("Col2","smallint",allowNulls:true)
            });

            //Create dataflow for loading data from csv into table
            CsvSource<string[]> source = new CsvSource<string[]>("C:\\Users\\Frank_Ye\\source\\repos\\ConsoleApp2\\ConsoleApp2\\input.csv");
            RowTransformation<string[], MyData> row = new RowTransformation<string[], MyData>(
                input =>
                    new MyData() { Col1 = input[0], Col2 = input[1] }
            );
            DbDestination<MyData> dest = new DbDestination<MyData>(dbConnection, "Table1");

            //Link components & run data flow
            source.LinkTo(row);
            row.LinkTo(dest);
            source.Execute();
            //dest.Wait();

            //Check if data exists in destination
            SqlTask.ExecuteReader(dbConnection, "select Col1, Col2 from Table1",
                col1 => Console.WriteLine(col1.ToString() + ","),
                col2 => Console.WriteLine(col2.ToString()));

            Console.WriteLine("Press any key to continue...");
            Console.ReadLine();
        }
    }
}