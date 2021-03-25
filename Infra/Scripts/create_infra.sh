
#!/bin/bash -e
location="${LOCATION}"
applicationName="${APPLICATION_NAME}"
dataLakeSasToken="${DATALAKE_SAS}"
dataLakeUri="${DATALAKE_URI}"
fileSystemName="${DATALAKE_FILESYSTEM}"

resourceGroupName="$applicationName-rg"
processorFunctionApp="$applicationName-processor-func"
fileAclFunctionApp="$applicationName-fileacl-func"
resultsFunctionApp="$applicationName-results-func"
processorStorageAccountName="$applicationName$RANDOM"requests
resultsStorageAccountName="$applicationName$RANDOM"results
fileAclStorageAccountName="$applicationName$RANDOM"fileacls
processorEventHubNamespace="$applicationName-processor-eh"
fileAclEventHubNamespace="$applicationName-fileacl-eh"
resultsEventHubNamespace="$applicationName-results-eh"

drain="FALSE"
resultsHub="results"
requestsHub="requests"
fileAclHub="fileacls"
pageSize="5000"

echo "Resource Group $resourceGroupName"
az group create -n $resourceGroupName -l $location --tags PendingDelete=true 

echo "EventHub Namespace $processorEventHubNamespace"
az eventhubs namespace create -g $resourceGroupName --name $processorEventHubNamespace -l $location --sku Standard --enable-auto-inflate --maximum-throughput-units 20
requestsEventHubConnection=$(az eventhubs namespace  authorization-rule keys list -g $resourceGroupName --namespace-name $processorEventHubNamespace  --name RootManageSharedAccessKey --query primaryConnectionString -o tsv)
ep="EntityPath=$requestsHub;SharedAccessKeyName"
requestsEventHubFullConnectionString=$(echo $requestsEventHubConnection | sed s/\SharedAccessKeyName/"$ep"/)

az eventhubs namespace create -g $resourceGroupName --name $resultsEventHubNamespace -l $location --sku Standard --enable-auto-inflate --maximum-throughput-units 20
resultsEventHubConnection=$(az eventhubs namespace  authorization-rule keys list -g $resourceGroupName --namespace-name $resultsEventHubNamespace  --name RootManageSharedAccessKey --query primaryConnectionString -o tsv)
ep="EntityPath=$resultsHub;SharedAccessKeyName"
resultsEventHubFullConnectionString=$(echo $resultsEventHubConnection | sed s/\SharedAccessKeyName/"$ep"/)

az eventhubs namespace create -g $resourceGroupName --name fileAclEventHubNamespace -l $location --sku Standard --enable-auto-inflate --maximum-throughput-units 20
fileAclEventHubConnection=$(az eventhubs namespace  authorization-rule keys list -g $resourceGroupName --namespace-name $fileAclEventHubNamespace  --name RootManageSharedAccessKey --query primaryConnectionString -o tsv)
ep="EntityPath=$fileAclHub;SharedAccessKeyName"
fileAclEventHubFullConnectionString=$(echo $fileAclEventHubConnection | sed s/\SharedAccessKeyName/"$ep"/)

az eventhubs eventhub create -g $resourceGroupName --namespace-name $processorEventHubNamespace --name $requestsHub --message-retention 4 --partition-count 32
az eventhubs eventhub create -g $resourceGroupName --namespace-name $resultsEventHubNamespace --name $resultsHub --message-retention 4 --partition-count 32
az eventhubs eventhub create -g $resourceGroupName --namespace-name $fileAclEventHubNamespace --name $fileAclHub --message-retention 4 --partition-count 32
az storage account create  --name $processorStorageAccountName  -l $location  -g $resourceGroupName  --sku Standard_LRS
az storage account create  --name $resultsStorageAccountName  -l $location  -g $resourceGroupName  --sku Standard_LRS
az storage account create  --name $fileAclStorageAccountName  -l $location  -g $resourceGroupName  --sku Standard_LRS
resultsStorageAccountConnectionString=$(az storage account show-connection-string -g $resourceGroupName -n $resultsStorageAccountName -o tsv)
az functionapp create  --name $processorFunctionApp   --consumption-plan-location $location  --storage-account $processorStorageAccountName  -g $resourceGroupName --functions-version 3
az functionapp create  --name $resultsFunctionApp   --consumption-plan-location $location  --storage-account $resultsStorageAccountName  -g $resourceGroupName --functions-version 3
az functionapp create  --name $fileAclFunctionApp   --consumption-plan-location $location  --storage-account $fileAclStorageAccountName  -g $resourceGroupName --functions-version 3
az webapp config appsettings set -g $resourceGroupName -n $processorFunctionApp --settings EventHubConnection=$requestsEventHubConnection DRAIN=$drain dataLakeSasToken=$dataLakeSasToken dataLakeUri=$dataLakeUri FileSystemName=$fileSystemName OutputStorageConnection=$resultsStorageAccountConnectionString RequestsEventHubFullConnectionString=$requestsEventHubFullConnectionString ResultsEventHubFullConnectionString=$resultsEventHubFullConnectionString FileAclEventHubFullConnectionString=$fileAclEventHubFullConnectionString ResultsHub=$resultsHub RequestsHub=$requestsHub PageSize=$pageSize
az webapp config appsettings set -g $resourceGroupName -n $resultsFunctionApp --settings EventHubConnection=$resultsEventHubConnection OutputStorageConnection=$resultsStorageAccountConnectionString ResultsHub=$resultsHub 
az webapp config appsettings set -g $resourceGroupName -n $resultsFunctionApp --settings EventHubConnection=$fileAclEventHubConnection FileAclHub=$fileAclHub DRAIN=$drain dataLakeSasToken=$dataLakeSasToken dataLakeUri=$dataLakeUri FileSystemName=$fileSystemName ResultsEventHubFullConnectionString=$resultsEventHubFullConnectionString
