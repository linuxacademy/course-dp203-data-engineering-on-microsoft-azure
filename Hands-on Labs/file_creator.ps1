##########################################################################
# Source: A Cloud Guru "DP-203 Data Engineering on Microsoft Azure" Course
# Author: Landon Fowler
# Purpose: Script to simulate streaming data into an Azure Data Lake.
#   It will continuously create JSON files in two different containers.
# Date Updated: 10/05/2020
##########################################################################

#Parameters
param(
        [string]$storageAccountName,
        [string]$storageAccountKey
    )

# Set initial variables
$containerNum = 1
$usaCounter = 1
$ukCounter =1

# Set storage context
$ctx = New-AzStorageContext -StorageAccountName $storageAccountName -StorageAccountKey $storageAccountKey

# Infinite Loop to simulate continuous stream
while (1 -lt 2) {  
    
    # Create files for the "usa" container
    if ($containerNum -eq 0) {
        $weatherObject = [PSCustomObject]@{
            country     = 'United States'
            high = Get-Random -Minimum 50 -Maximum 100
            low = Get-Random -Minimum 0 -Maximum 50
        }

        # Name, create, and upload the source file
        $sourceFile = "weather_usaUser$usaCounter.json"
        $weatherObject | ConvertTo-Json > $sourceFile
        New-AzDataLakeGen2Item -Context $ctx -FileSystem "usa" -Path $sourceFile -Source $sourceFile -Force

        # Remove the source file after we're finished with it
        Remove-Item $sourceFile

        # Swap to the UK for the next round
        $containerNum = 1

        # Increment the user number to ensure unique file names
        $usaCounter++
    }
    # Create files for the "uk1" container
    elseif ($containerNum -eq 1) {
        $weatherObject = [PSCustomObject]@{
            country     = 'United Kingdom'
            high = Get-Random -Minimum 40 -Maximum 80
            low = Get-Random -Minimum 0 -Maximum 40
        }

        # Name, create, and upload the source file
        $sourceFile = "weather_ukUser$ukCounter.json"
        $weatherObject | ConvertTo-Json > $sourceFile
        New-AzDataLakeGen2Item -Context $ctx -FileSystem "uk1" -Path $sourceFile -Source $sourceFile -Force

        # Remove the source file after we're finished with it
        Remove-Item $sourceFile

        # Swap to the UK for the next round
        $containerNum = 0

        # Increment the user number to ensure unique file names
        $ukCounter++
    }

    # Sleep between 1-5 seconds so that it's more like random users uploading and less like a machine fire hose
    Start-Sleep -Seconds (Get-Random -Minimum 1 -Maximum 5)

}