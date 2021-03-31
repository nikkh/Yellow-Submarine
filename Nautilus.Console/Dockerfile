#See https://aka.ms/containerfastmode to understand how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/runtime:3.1 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:3.1 AS build
WORKDIR /src
COPY ["Nautilus.Console/Nautilus.Console.csproj", "Nautilus.Console/"]
RUN dotnet restore "Nautilus.Console/Nautilus.Console.csproj"
COPY . .
WORKDIR "/src/Nautilus.Console"
RUN dotnet build "Nautilus.Console.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "Nautilus.Console.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "Nautilus.Console.dll"]