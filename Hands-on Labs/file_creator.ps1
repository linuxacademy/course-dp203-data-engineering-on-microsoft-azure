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


# Infinite Loop to reproduce continuous stream
while (1 -lt 2) {
    
    
    if ($containerNum -eq 0) {
        $weatherObject = [PSCustomObject]@{
            country     = 'United States'
            high = Get-Random -Minimum 50 -Maximum 100
            low = Get-Random -Minimum 0 -Maximum 50
        }

        $sourceFile = "weather_usaUser$usaCounter.json"
        $weatherObject | ConvertTo-Json > $sourceFile
        New-AzDataLakeGen2Item -Context $ctx -FileSystem "usa" -Path $sourceFile -Source $sourceFile -Force

        Remove-Item $sourceFile

        $containerNum = 1
        $usaCounter++
    }
    elseif ($containerNum -eq 1) {
        $weatherObject = [PSCustomObject]@{
            country     = 'United Kingdom'
            high = Get-Random -Minimum 40 -Maximum 80
            low = Get-Random -Minimum 0 -Maximum 40
        }

        $sourceFile = "weather_ukUser$ukCounter.json"
        $weatherObject | ConvertTo-Json > $sourceFile
        New-AzDataLakeGen2Item -Context $ctx -FileSystem "uk1" -Path $sourceFile -Source $sourceFile -Force

        Remove-Item $sourceFile

        $containerNum = 0
        $ukCounter++
    }


    Start-Sleep -Seconds 2

}