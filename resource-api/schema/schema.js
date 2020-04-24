const graphql = require('graphql');

const {GraphQLObjectType, GraphQLString, GraphQLList, GraphQLSchema, GraphQLInt, GraphQLUnionType} = graphql;
const _ = require('lodash');
const SgciResource = require("../models/sgciResource");
const mongoose = require("mongoose");

const SgciResourceType = new GraphQLObjectType({
    name: 'sgciResource',
    fields: () => ({
        id: {type: GraphQLString},
        name: {type: GraphQLString},
        description: {type: GraphQLString},
        resourceType: {type: GraphQLString},
        resource: {type: ResourceUnionType},
        hosts: {type: new GraphQLList(HostType)},
        connections: {type: new GraphQLList(ConnectionType)}
    })
});

const ResourceResolveType = (data) => {
    if (data.storageType)
        return StorageType
    if (data.schedulerType)
        return ComputeType
}

const ResourceUnionType = new GraphQLUnionType({
    name: 'resource',
    types: () => ([StorageType, ComputeType]),
    resolveType: ResourceResolveType
});

const HostType = new GraphQLObjectType({
    name: 'host',
    fields: () => ({
        hostname: {type: GraphQLString},
        ip: {type: GraphQLString},
        priority: {type: GraphQLInt},
    })
});

const StorageType = new GraphQLObjectType({
    name: 'storage',
    fields: () => ({
        storageType: {type: GraphQLString},
        connection: {type: ConnectionType},
        fileSystems: {type: new GraphQLList(FileSystemType)},
        capacity: {type: CapacityType},
        quota: {type: QuotaType}
    })
});

const CapacityType = new GraphQLObjectType({
    name: 'capacity',
    fields: () => ({
        totalBytes: {type: GraphQLInt}
    })
});

const QuotaType = new GraphQLObjectType({
    name: 'quota',
    fields: () => ({
        bytesPerUser: {type: GraphQLInt}
    })
});

const FileSystemType = new GraphQLObjectType({
    name: 'fileSystem',
    fields: () => ({
        rootDir: {type: GraphQLString},
        homeDir: {type: GraphQLString},
        scratchDir: {type: GraphQLString},
        workDir: {type: GraphQLString},
        archiveDir: {type: GraphQLString}
    })
});

const ComputeType = new GraphQLObjectType({
    name: 'compute',
    fields: () => ({
        schedulerType: {type: GraphQLString},
        connection: {type: ConnectionType},
        executionCommands: {type: new GraphQLList(ExecutionCommandType)},
        batchSystem: {type: BatchSystemType},
        forkSystem: {type: ForkSystemType}
    })
})

const ConnectionType = new GraphQLObjectType({
    name: 'connection',
    fields: () => ({
        connectionProtocol: {type: GraphQLString},
        securityProtocol: {type: GraphQLString},
        port: {type: GraphQLInt},
        proxyHost: {type: GraphQLString},
        proxyPort: {type: GraphQLInt}
    })
});

const ExecutionCommandType = new GraphQLObjectType({
    name: 'executionCommand',
    fields: () => ({
        commandType: {type: GraphQLString},
        commandPrefix: {type: GraphQLString},
        moduleDependencies: {type: new GraphQLList(GraphQLString)}
    })
})

const BatchSystemType = new GraphQLObjectType({
    name: 'batchSystem',
    fields: () => ({
        jobManager: {type: GraphQLString},
        commandPaths: {type: new GraphQLList(CommandPathType)},
        partitions: {type: new GraphQLList(PartitionType)}
    })
})

const CommandPathType = new GraphQLObjectType({
    name: 'commandPath',
    fields: () => ({
        name: {type: GraphQLString}
    })
});

const PartitionType = new GraphQLObjectType({
    name: 'partition',
    fields: () => ({
        name: {type: GraphQLString},
        totalNodes: {type: GraphQLInt},
        nodeHardware: {type: NodeHardwareType},
        computeQuotas: {type: ComputeQuotaType}
    })
});

const NodeHardwareType = new GraphQLObjectType({
    name: 'nodeHardware',
    fields: () => ({
        cpuType: {type: GraphQLString},
        cpuCount: {type: GraphQLInt},
        gpuType: {type: GraphQLString},
        gpuCount: {type: GraphQLInt},
        memoryType: {type: GraphQLString},
        memorySize: {type: GraphQLString}
    })
});

const ComputeQuotaType = new GraphQLObjectType({
    name: 'computeQuota',
    fields: () => ({
        maxJobsTotal: {type: GraphQLInt},
        maxJobsPerUser: {type: GraphQLInt},
        maxNodesPerJob: {type: GraphQLInt},
        maxTimePerJob: {type: GraphQLInt},
        maxMemoryPerJob: {type: GraphQLString},
        maxCPUsPerJob: {type: GraphQLInt},
        maxGPUsPerJob: {type: GraphQLInt}
    })
});

const ForkSystemType = new GraphQLObjectType({
    name: 'forkSystem',
    fields: () => ({
        systemType: {type: GraphQLString},
        version: {type: GraphQLString},
        nodeHardware: {type: NodeHardwareType}
    })
});

async function getSgciResources(key, value) {
    const res = await SgciResource.find({resourceType: "STORAGE"}).limit(1).exec()
    print(res)
}

const RootQuery = new GraphQLObjectType({
    name: 'RootQueryType',
    fields: {
        resource: {
            type: GraphQLList(SgciResourceType),
            args: {id: {type: GraphQLString}, name: {type: GraphQLString}, resourceType: {type: GraphQLString}},
            resolve(parent, args) {
                console.log(args)

                query = {}
                
                if (args.name) {
                    console.log("Name found")
                    query["name"] = args.name;
                }

                if (args.resourceType) {
                    console.log("Storage type found")
                    query["resourceType"] = args.resourceType
                }

                if (args.id) {
                    console.log("Id found")
                    query["id"] = args.id
                }

                return SgciResource.find(query);
            }
        }
    } 
});

module.exports = new GraphQLSchema({
    query: RootQuery
})