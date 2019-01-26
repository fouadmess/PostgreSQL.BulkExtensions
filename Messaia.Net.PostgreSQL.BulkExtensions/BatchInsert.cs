///-----------------------------------------------------------------
///   Author:         Fouad Messaia
///   AuthorUrl:      http://messaia.com
///   Date:           26.01.2019
///   Copyright (©)   2019, MESSAIA.NET, all Rights Reserved. 
///                   Licensed under the Apache License, Version 2.0. 
///                   See License.txt in the project root for license information.
///-----------------------------------------------------------------
namespace Messaia.Net.PostgreSQL.BulkExtensions
{
    using Microsoft.EntityFrameworkCore;
    using Microsoft.EntityFrameworkCore.Metadata;
    using Microsoft.EntityFrameworkCore.Storage;
    using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
    using Npgsql;
    using Npgsql.EntityFrameworkCore.PostgreSQL.Storage.Internal;
    using Npgsql.EntityFrameworkCore.PostgreSQL.Storage.Internal.Mapping;
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;
    using System.Reflection;

    /// <summary>
    /// BatchInsert class.
    /// </summary>
    public static class BatchInsert
    {
        /// <summary>
        /// Inserts multiple records
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="dbContext"></param>
        /// <param name="entities"></param>
        public static int Execute<TEntity>(DbContext dbContext, IEnumerable<TEntity> entities)
        {
            /* Check dbContext against null */
            if (dbContext == null)
            {
                throw new ArgumentNullException(nameof(dbContext));
            }

            /* Check entities against null */
            if (entities == null)
            {
                throw new ArgumentNullException(nameof(entities));
            }

            /* NoOp */
            if (entities.Count() == 0)
            {
                return 0;
            }

            /* Get the underlying ADO.NET DbConnection for this DbContext */
            var connection = dbContext.Database.GetDbConnection();

            /* Open a database connection, if not done already */
            if (connection.State != ConnectionState.Open)
            {
                connection.Open();
            }

            /* Get the type of the entity */
            var classType = typeof(TEntity);

            /* Gets the type of the entity that maps the given entity class */
            var entityType = dbContext.Model.FindEntityType(classType);

            /* Gets the relational database specific metadata for the specified entity */
            var tableMetadata = entityType.Relational();

            /* Type mapper */
            var typeMappingSource = new NpgsqlTypeMappingSource(
                new TypeMappingSourceDependencies(new ValueConverterSelector(new ValueConverterSelectorDependencies()),
                Array.Empty<ITypeMappingSourcePlugin>()),
                new RelationalTypeMappingSourceDependencies(Array.Empty<IRelationalTypeMappingSourcePlugin>()),
                new NpgsqlSqlGenerationHelper(new RelationalSqlGenerationHelperDependencies()),
                null
            );

            /* Get column info  */
            var properties = entityType.GetProperties()
                .Where(x => !x.IsPrimaryKey())
                .Select(x =>
                {
                    /* Gets the relational database specific metadata for the current property */
                    var propertyMetadata = x.Relational();
                    return new
                    {
                        PropertyName = x.Name,
                        PropertyType = x.ClrType,
                        PropertyInfo = classType.GetProperty(x.Name),
                        propertyMetadata.ColumnName,
                        NpgsqlType = (typeMappingSource.FindMapping(x.ClrType) as NpgsqlTypeMapping)?.NpgsqlDbType,
                        propertyMetadata.ColumnType,
                        x.IsNullable
                    };
                })
                .ToList();

            /* Build the COPY command */
            var command = string.Format(
                @"COPY {0}""{1}"" ({2}) FROM STDIN BINARY;",
                string.IsNullOrWhiteSpace(tableMetadata.Schema) ? string.Empty : $"{tableMetadata.Schema}.",
                tableMetadata.TableName,
                properties.Select(x => $@"""{x.ColumnName}""").Aggregate((c, n) => $"{c}, {n}")
            );

            /* Begin a binary COPY FROM STDIN operation */
            using (var writer = (connection as NpgsqlConnection).BeginBinaryImport(command))
            {
                foreach (var entity in entities)
                {
                    if (entity == null)
                    {
                        continue;
                    }

                    /* Start writing a single row */
                    writer.StartRow();

                    foreach (var property in properties)
                    {
                        /* Get property value */
                        var value = property.PropertyInfo.GetValue(entity, null);

                        /* Write null */
                        if (value == null)
                        {
                            writer.WriteNull();
                            continue;
                        }

                        if (property.NpgsqlType != null)
                        {
                            writer.Write(value, property.NpgsqlType.Value);
                        }
                        else
                        {
                            writer.Write(value is Enum ? (int)value : value, property.ColumnType);
                        }
                    }
                }

                /* Complete the import operation */
                writer.Complete();
            }

            return entities.Count();
        }

        /// <summary>
        /// Inserts multiple records
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="dbContext"></param>
        /// <param name="entities"></param>
        /// <param name="schema"></param>
        public static int Execute<TEntity>(DbContext dbContext, string tableName, IEnumerable<TEntity> entities, string schema = null)
        {
            /* Check dbContext against null */
            if (dbContext == null)
            {
                throw new ArgumentNullException(nameof(dbContext));
            }

            /* Check entities against null */
            if (entities == null)
            {
                throw new ArgumentNullException(nameof(entities));
            }

            /* NoOp */
            if (entities.Count() == 0)
            {
                return 0;
            }

            /* Get the underlying ADO.NET DbConnection for this DbContext */
            var connection = dbContext.Database.GetDbConnection();

            /* Open a database connection, if not done already */
            if (connection.State != ConnectionState.Open)
            {
                connection.Open();
            }

            /* Get the type of the entity */
            var classType = entities.FirstOrDefault()?.GetType();

            /* Gets the relational database specific metadata for the specified entity */
            var tableMetadata = dbContext.Model
                .GetEntityTypes()
                .Select(x => x.Relational())
                .FirstOrDefault(x => x.TableName.Equals(tableName));

            /* Gets the type of the entity that maps the given entity class */
            var entityType = tableMetadata.GetType()
                .GetProperty("EntityType", BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(tableMetadata) as IEntityType;

            /* Type mapper */
            var typeMappingSource = new NpgsqlTypeMappingSource(
                new TypeMappingSourceDependencies(new ValueConverterSelector(new ValueConverterSelectorDependencies()),
                Array.Empty<ITypeMappingSourcePlugin>()),
                new RelationalTypeMappingSourceDependencies(Array.Empty<IRelationalTypeMappingSourcePlugin>()),
                new NpgsqlSqlGenerationHelper(new RelationalSqlGenerationHelperDependencies()),
                null
            );

            /* Get column info  */
            var properties = entityType.GetProperties()
                .Where(x => !x.IsPrimaryKey())
                .Select(x =>
                {
                    /* Gets the relational database specific metadata for the current property */
                    var propertyMetadata = x.Relational();
                    return new
                    {
                        PropertyName = x.Name,
                        PropertyType = x.ClrType,
                        PropertyInfo = classType.GetRuntimeProperty(x.Name),
                        propertyMetadata.ColumnName,
                        NpgsqlType = (typeMappingSource.FindMapping(x.ClrType) as NpgsqlTypeMapping)?.NpgsqlDbType,
                        propertyMetadata.ColumnType,
                        x.IsNullable
                    };
                })
                .ToList();

            /* Build the COPY command */
            var command = string.Format(
                @"COPY {0}""{1}"" ({2}) FROM STDIN BINARY;",
                string.IsNullOrWhiteSpace(schema) ? string.Empty : $"{schema}.",
                tableName,
                properties.Select(x => $@"""{x.ColumnName}""").Aggregate((c, n) => $"{c}, {n}")
            );

            /* Begin a binary COPY FROM STDIN operation */
            using (var writer = (connection as NpgsqlConnection).BeginBinaryImport(command))
            {
                foreach (var entity in entities)
                {
                    if (entity == null)
                    {
                        continue;
                    }

                    /* Start writing a single row */
                    writer.StartRow();

                    foreach (var property in properties)
                    {
                        /* Get property value */
                        var value = property.PropertyInfo?.GetValue(entity, null);

                        /* Write null */
                        if (value == null)
                        {
                            writer.WriteNull();
                            continue;
                        }

                        if (property.NpgsqlType != null)
                        {
                            writer.Write(value, property.NpgsqlType.Value);
                        }
                        else
                        {
                            writer.Write(value is Enum ? (int)value : value, property.ColumnType);
                        }
                    }
                }

                /* Complete the import operation */
                writer.Complete();
            }

            return entities.Count();
        }
    }
}