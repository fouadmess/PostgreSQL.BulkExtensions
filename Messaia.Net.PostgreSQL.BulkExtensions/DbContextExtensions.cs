///-----------------------------------------------------------------
///   Author:         Fouad Messaia
///   AuthorUrl:      http://messaia.com
///   Date:           26.01.2019
///   Copyright (©)   2019, MESSAIA.NET, all Rights Reserved. 
///                   Licensed under the Apache License, Version 2.0. 
///                   See License.txt in the project root for license information.
///-----------------------------------------------------------------
namespace Microsoft.EntityFrameworkCore
{
    using Messaia.Net.PostgreSQL.BulkExtensions;
    using System.Collections.Generic;

    /// <summary>
    /// DbContextExtensions class
    /// </summary>
    public static class DbContextExtensions
    {
        /// <summary>
        /// Inserts multiple records
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="dbContext"></param>
        /// <param name="entities"></param>
        public static int BulkInsert<TEntity>(this DbContext dbContext, IEnumerable<TEntity> entities)
        {
            return BatchInsert.Execute(dbContext, entities);
        }

        /// <summary>
        /// Inserts multiple records
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="dbContext"></param>
        /// <param name="tableName"></param>
        /// <param name="entities"></param>
        /// <param name="schema"></param>
        public static int BulkInsert<TEntity>(this DbContext dbContext, string tableName, IEnumerable<TEntity> entities, string schema = null)
        {
            return BatchInsert.Execute(dbContext, tableName, entities, schema);
        }
    }
}