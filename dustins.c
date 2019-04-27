#include "embedded.h"
#include <stdio.h>
#include <pthread.h>
#include <stdlib.h>

#define error(msg) {fprintf(stderr, "Failure: %s\n", msg); return -1;}

void* editRow(void* conn) {

    char* err;

    err = monetdb_query((char*)conn, "UPDATE test SET category = 'newval' WHERE app = 'Coloring book moana';",
                        1, NULL, NULL, NULL);
    if (err != 0) {
        return (void*)err;
    }

    return (void*)err;
}

int main(int argc, char *argv[]) {

    int numthreads = atoi(argv[1]);

    char* err = 0;
    void* conn = 0;
    monetdb_result* result = 0;
    size_t r, c;

    // first argument is a string for the db directory or NULL for in-memory mode
    // we should try performance tests with both!
    err = monetdb_startup(NULL, 0, 0);
    if (err != 0) {
        error(err)
    }

    conn = monetdb_connect();
    if (conn == NULL) {
        error("Connection failed")
    }

    err = monetdb_query(conn, "CREATE TABLE test (app string, category string, rating string, reviews string,"
                              "size string, installs string, type string, price string, contentR string,"
                              "genres string, lastU string, curV string, andV string)", 1, NULL, NULL, NULL);
    if (err != 0) {
        error(err)
    }

    err = monetdb_query(conn, "COPY 100 RECORDS INTO test FROM '/users/dgal/googlePlay/googleplaystore.csv'"
                              "USING DELIMITERS ',','\\n','\"' NULL AS '';", 1, NULL, NULL, NULL);
    if (err != 0) {
        error(err)
    }

    //
    // dustin's multi-threading code
    //

    pthread_t id[numthreads];
    int i,rc;

    for (i = 0; i < numthreads; i++) {
        rc = pthread_create(&id[i], NULL, &editRow, conn);
        if (rc != 0) {
            printf("Thread creation failed.");
            return 0;
        }
    }

    for (i = 0; i < numthreads; i++) {
        pthread_join(id[i], NULL);
    }

    err = monetdb_query(conn, "SELECT * FROM test WHERE app = 'Coloring book moana';", 1, &result, NULL, NULL);
    if (err != 0) {
        error(err)
    }

    fprintf(stdout, "Query result with %d cols and %d rows\n", (int) result->ncols, (int) result->nrows);

    for (r = 0; r < result->nrows; r++) {
        for (c = 0; c < result->ncols; c++) {
            monetdb_column* rcol = monetdb_result_fetch(result, c);
            switch (rcol->type) {
                case monetdb_int32_t: {
                    monetdb_column_int32_t * col = (monetdb_column_int32_t *) rcol;
                    if (col->data[r] == col->null_value) {
                        printf("NULL");
                    } else {
                        printf("%d", (int) col->data[r]);
                    }
                    break;
                }
                case monetdb_str: {
                    monetdb_column_str * col = (monetdb_column_str *) rcol;
                    if (col->is_null(col->data[r])) {
                        printf("NULL");
                    } else {
                        printf("%s", (char*) col->data[r]);
                    }
                    break;
                }
                default: {
                    printf("UNKNOWN");
                }
            }
            if (c + 1 < result->ncols) {
                printf(", ");
            }
        }
        printf("\n");
    }

    monetdb_cleanup_result(conn, result);
    monetdb_disconnect(conn);
    monetdb_shutdown();

    return 0;
}
