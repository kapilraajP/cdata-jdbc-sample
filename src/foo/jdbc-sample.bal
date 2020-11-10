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

public function main() {
    
    // JDBC client created using CData JDBC driver. 
    jdbc:Client|sql:Error cdataSalesforceDB = new (
    "jdbc:salesforce:User=" + config:getAsString("USERNAME") + ";Password=" + config:getAsString("PASSWORD") + ";Security Token=" + config:getAsString("TOKEN")
    );

    if (cdataSalesforceDB is sql:Error) {
        io:println("JDBC initialization failed!");
    }
    else{
        // Retrieve all records of Salesforce Accounts
        stream<record{}, sql:Error> resultStream = cdataSalesforceDB->query("SELECT Id, Name, Type, AccountNumber, Industry, Description FROM Account WHERE Id IS NOT NULL");
        
        error? e = resultStream.forEach(function(record {} result) {
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

        sql:ParameterizedQuery insertQuery= `INSERT INTO Account (Name, Type, AccountNumber, Industry, Description) VALUES (${sampleAccount.name},${sampleAccount?.accType ?: ""},${sampleAccount?.accountNumber ?: ""},${sampleAccount?.industry ?: ""},${sampleAccount?.description ?: ""})`;
        sql:ExecutionResult|sql:Error result = cdataSalesforceDB->execute(insertQuery);
        if (result is sql:ExecutionResult) {
            io:println(result);
            sampleAccount.id=<string>result.lastInsertId;
        }else{
            io:println("Insert failed: ", <string>result.message());
        }


        // Update a Salesforce Account record
        sql:ParameterizedQuery updateQuery = `UPDATE Account SET name = ${"Updated Account"} WHERE id = ${sampleAccount.id}`;
        result = cdataSalesforceDB->execute(updateQuery);
        if (result is sql:ExecutionResult) {
                io:println(result.toString());
        } else {
            io:println("Update data from Salesforce Accounts table has failed: ",<string>result.message());
        }


        // Select a Salesforce Account recort
        resultStream = cdataSalesforceDB->query("SELECT Id, Name, Type, AccountNumber, Industry, Description FROM Account WHERE Id =" + sampleAccount.id);
        
        e = resultStream.forEach(function(record {} result) {
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

   }
}


