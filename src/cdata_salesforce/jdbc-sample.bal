import ballerina/io;
import ballerina/java.jdbc;
import ballerina/sql;
import ballerina/config;


type SalesforceAccount record {
    string id; 
    string name; 
    string? accType?; 
    string? accountNumber?;
    string? industry?;
    string? description?; 
};

public function main() returns @tainted error?{
    
    // JDBC client created using CData JDBC driver. 
    jdbc:Client|sql:Error cdataSalesforceDB = check new (
        "jdbc:salesforce:User=" + config:getAsString("SALESFORCE_USERNAME") + ";Password=" + config:getAsString("PASSWORD") + ";Security Token=" + config:getAsString("TOKEN")
    );

    if(cdataSalesforceDB is sql:Error){
        io:println("Initialization failed");
    }else{
        // Retrieve all records of Salesforce Accounts
        sql:ParameterizedQuery selectQuery = `SELECT Id, Name, AccountNumber, Industry, Description 
                                                FROM Account WHERE Id IS NOT NULL`;
        stream<record{}, sql:Error> resultStream = cdataSalesforceDB->query(selectQuery,SalesforceAccount);

        stream<SalesforceAccount, sql:Error> accountResultStream = <stream<SalesforceAccount, sql:Error>> resultStream;
            
        error? e = accountResultStream.forEach(function(SalesforceAccount result) {
            io:println("Account details: ", result);
        });

        if (e is error) {
            io:println("Fetching data in stream by foreach failed!");
            io:println(e);
        }
        e = resultStream.close();
        

        // Add a new Salesforce Account record
        SalesforceAccount sampleAccount = {
            id: "ACC_000000",
            name: "Test Account", 
            accType: "Customer - Direct", 
            accountNumber: "CD355119-TEST",
            industry: "Energy", 
            description: "Test account desc."
        };

        sql:ParameterizedQuery insertQuery= `INSERT INTO Account (Name, Type, AccountNumber, Industry, Description) 
                                            VALUES (${sampleAccount.name},${sampleAccount?.accType ?: ""},${sampleAccount?.accountNumber ?: ""},
                                            ${sampleAccount?.industry ?: ""},${sampleAccount?.description ?: ""})`;
        sql:ExecutionResult|sql:Error result = cdataSalesforceDB->execute(insertQuery);
        if (result is sql:ExecutionResult) {
            io:println(result);
            sampleAccount.id=<string>result.lastInsertId;
        } else{
            io:println("Insert failed: ", <string>result.message());
        }


        // Update a Salesforce Account record
        sql:ParameterizedQuery updateQuery = `UPDATE Account SET name = ${"Updated Account"} WHERE id = ${sampleAccount.id}`;
        result = cdataSalesforceDB->execute(updateQuery);
        if (result is sql:ExecutionResult) {
            io:println(result.toString());
        } else{
            io:println("Update data from Salesforce Accounts table has failed: ",<string>result.message());
        }


        // Select a Salesforce Account record
        selectQuery = `SELECT Id, Name, AccountNumber, Industry, Description FROM Account WHERE Id = ${sampleAccount.id};`;
        resultStream = cdataSalesforceDB->query(selectQuery,SalesforceAccount);

        accountResultStream = <stream<SalesforceAccount, sql:Error>> resultStream;
            
        e = accountResultStream.forEach(function(SalesforceAccount result) {
            io:println("Account details: ", result);
        });

        if (e is error) {
            io:println("Fetching data in stream by foreach failed!");
            io:println(e);
        }
        e = resultStream.close();


        // Delete a new Salesforce Account record
        sql:ParameterizedQuery deleteQuery = `DELETE FROM Account WHERE id = ${sampleAccount.id}`;
        result = cdataSalesforceDB->execute(deleteQuery);
        if (result is sql:ExecutionResult) {
            io:println(result.toString());
        } else {
            io:println("Delete data from Salesforce Accounts table has failed: ",<string>result.message());
        }
            
            
        //Batch Processing Insert
        SalesforceAccount sampleAccount1 = {
            id: "ACC_000001",
            name: "Test Account-Batch1", 
            accType: "Customer - Direct", 
            accountNumber: "CD355119-TEST",
            industry: "Energy", 
            description: "Test account desc."
        };
         SalesforceAccount sampleAccount2 = {
            id: "ACC_000002",
            name: "Test Account-Batch2", 
            accType: "Customer - Direct", 
            accountNumber: "CD355119-TEST",
            industry: "Energy", 
            description: "Test account desc."
        };
        SalesforceAccount sampleAccount3 = {
            id: "ACC_000003",
            name: "Test Account-Batch3", 
            accType: "Customer - Direct", 
            accountNumber: "CD355119-TEST",
            industry: "Energy", 
            description: "Test account desc."
        };

        var batchRecords = [sampleAccount1, sampleAccount2, sampleAccount3];

        sql:ParameterizedQuery[] insertQueries =
        from var data in batchRecords
            select  `INSERT INTO Account
                    (Name, Type, AccountNumber, Industry, Description)
                    VALUES (${data.name}, ${data?.accType},
                    ${data?.accountNumber}, ${data?.industry}, ${data?.description})`;

        sql:ExecutionResult[]|sql:Error batchResults = cdataSalesforceDB->batchExecute(insertQueries);
        string[] idRecords=[];

        if (batchResults is sql:ExecutionResult[]) {
            io:println("Batch insert success");
            foreach var batchResult in batchResults {
                idRecords.push(<string>batchResult.lastInsertId);
                io:println("Inserted ID: ", batchResult.lastInsertId);
            }
        } else {
            io:println("Error occurred while batch insert execution: ", result);
        }

        //Batch Update a Salesforce Account records
        sql:ParameterizedQuery[] updateQueries =
        from var data in idRecords
            select `UPDATE Account SET name = ${"Batch Updated Account"} WHERE id = ${data}`;

        batchResults = cdataSalesforceDB->batchExecute(updateQueries);

        if (batchResults is sql:ExecutionResult[]) {
            io:println("Batch update success");
            foreach var batchResult in batchResults {
                io:println("Updated ID: ", batchResult.lastInsertId);
            }
        } else {
            io:println("Error occurred while batch update execution: ", result);
        }


        //Batch Delete a Salesforce Account records
        sql:ParameterizedQuery[] deleteQueries =
        from var data in idRecords
            select `DELETE FROM Account WHERE id = ${data}`;

        batchResults = cdataSalesforceDB->batchExecute(deleteQueries);

        if (batchResults is sql:ExecutionResult[]) {
            io:println("Batch delete success");
            foreach var batchResult in batchResults {
                io:println("Deleted ID: ", batchResult.lastInsertId);
            }
        } else {
            io:println("Error occurred while batch delete execution: ", result);
        }


        //Usage of Stored procedures
        sql:ProcedureCallResult|sql:Error retCall = 
                                cdataSalesforceDB->call("{CALL GetUserInformation()}");

        if (retCall is sql:ProcedureCallResult) {
            io:println("Calling stored procedure");
            stream<record{}, sql:Error>? resultStoredProcedure = retCall.queryResult;
            if (!(resultStoredProcedure is ())) {
                stream<record{}, sql:Error> userStream = 
                                        <stream<record{}, sql:Error>> resultStoredProcedure;
                e = userStream.forEach(function(record{} user) {
                                    io:println("User details: ", user.toJson());
                            });
            } else {
                io:println("Empty result is returned from the `GetUserInformation`.");
            }

        } else {
            io:println("Error occurred while invoking `GetUserInformation`.");
        }

        check cdataSalesforceDB.close();
    }

}


