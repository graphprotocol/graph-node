diesel::table! {
    public.graphman_command_executions {
        id -> BigSerial,
        kind -> Varchar,
        status -> Varchar,
        error_message -> Nullable<Varchar>,
        created_at -> Timestamptz,
        updated_at -> Nullable<Timestamptz>,
        completed_at -> Nullable<Timestamptz>,
    }
}
