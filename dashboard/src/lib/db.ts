import postgres from "postgres";

// Singleton connection pool
let sql: ReturnType<typeof postgres> | null = null;

export function getDb() {
  if (sql) return sql;

  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) {
    throw new Error(
      "DATABASE_URL environment variable is not set. " +
        "Add it to dashboard/.env.local or set it in your environment."
    );
  }

  sql = postgres(connectionString, {
    max: 10,
    idle_timeout: 20,
    connect_timeout: 10,
    // Supabase pooler sometimes needs this
    prepare: false,
  });

  return sql;
}
