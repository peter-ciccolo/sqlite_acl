## ACL System for SQLite

- Exposes API for:
    - Query() that takes a SQL string and returns a list of rows (mapping columns to values)
    - Admin functions:
        - Adding permissions to a user
        - Removing permissions from a user
        - Fetching permissions for a user or for all users
        - Permissions specify: R/W and table, and are either blanket permissions (all rows) or a specified subset of PKs
            - Permissions are only defined in the positive for simplicity
- Backing DB and backing ACL store are both modular
 interfaces
    - Implementations for SQLite for both.
    - Default server would use the same SQLite for both but this is not necessary.
    - ACL store is not required to be a SQL database.
    - In-memory implementation for testing would be trivial.
- ACL changes are write-through to the backing store
- Checking query against ACLs is based on constructing the set of required permissions for the query by walking an AST parsed from the SQL.
- User ids and keys are separated so that admins do not need users' keys to refer to them, and to allow for future key rotation.

## Known Gaps
- Joins are not handled correctly(!)
- Some CTEs may not be handled correctly (further testing neeed)
- The outermost layer of server code is not implemented (translating between JSON requests/responses and internal objects, routing).
- Logging is not implemented
- Creation of new users and promotion/demotion of admins must be handled directly in the ACL store (out of scope)
- Key rotation is not implemented (out of scope)
- Authentication is not implemented (out of scope)


## Distributed Version
- Since the ACL logic is fairly lightweight, it could be run as a sidecar to the DB process, with users talking to the ACL controller and the ACL controller relaying to the DB.
    - This means the ACL server availaibility scales with the number of required DB servers automatically.
- The bottleneck then is not the backing DB for the ACL server, but the backing store for the ACL
    - Unlike in the single-homed version, the backing store cannot be the same DB, since this fragments ACL updates.
    - Redis or similar might be a good backing store to be shared by the ACL servers, since:
        - it is fast
        - it is fairly durable
        - the total amount of ACL data to store is low
- The code currently assumes that ACLs do not change except by calls to this process, which is true in the single-home version. To allow for distributed processes, we should use a cache for the ACLs
    - This cache can have a relatively long cache eviction so that the DB does not become unavailable if the ACL backing store goes down briefly
    - It should have a much shorter time to refresh values from the backing store, to allow for updates to propagate quickly.