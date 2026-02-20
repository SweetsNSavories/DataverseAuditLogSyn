# Multi-stage build for Dataverse Audit Sync console app

# Build stage
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

COPY ["DataverseAuditJob.csproj", "./"]
RUN dotnet restore "DataverseAuditJob.csproj"

COPY . .
RUN dotnet build "DataverseAuditJob.csproj" -c Release -o /app/build

RUN dotnet publish "DataverseAuditJob.csproj" -c Release -o /app/publish

# Runtime stage
FROM mcr.microsoft.com/dotnet/runtime:8.0

WORKDIR /app
COPY --from=build /app/publish .

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD dotnet /app/DataverseAuditJob.dll || exit 1

ENTRYPOINT ["dotnet", "DataverseAuditJob.dll"]
