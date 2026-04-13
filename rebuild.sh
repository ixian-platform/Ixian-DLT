#!/bin/sh -e
echo Rebuilding Ixian DLT...

echo Checking .NET SDK Version
# Get the current active SDK version
DOTNET_VER=$(dotnet --version 2>/dev/null)

if [ -z "$DOTNET_VER" ]; then
    echo "Error: .NET is not installed. .NET 10 is required to build Ixian DLT from source."
    exit 1
fi

# Extract the major version (everything before the first dot)
MAJOR_VER=$(echo "$DOTNET_VER" | cut -d. -f1)

if [ "$MAJOR_VER" -lt 10 ]; then
    echo "Error: .NET $MAJOR_VER detected. .NET 10 is required to build Ixian DLT from source."
    exit 1
fi

echo Cleaning previous build
dotnet clean --configuration Release
echo Restoring packages
dotnet restore
echo Building DLT
dotnet build --configuration Release -p WarningLevel=0
echo Cleaning Argon2
cd Argon2_C
make clean
echo Building Argon2
make
echo Copying Argon2 library to release directory
cp libargon2.so.1 ../IxianDLT/bin/Release/net10.0/libargon2.so
cd ..
echo Done rebuilding Ixian DLT