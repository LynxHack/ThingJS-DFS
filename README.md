# Distributed-File-System

## Implementation Details
https://courses.cs.washington.edu/courses/cse490h/11wi/CSE490H_files/gfs.pdf

### Read Algorithm
1. Application originates the read request.
2. GFS client translates the request from (filename, byte range) -> (filename, chunk index), and sends it to master.
3. Master responds with chunk handle and replica locations (i.e. chunkservers where the replicas are stored).
4. Client picks a location and sends the (chunk handle, byte range) request to that location.
5. Chunkserver sends requested dat to the client.
6. Client forwards the data to the application.


### Write Algorithm
1. Application originates write request.
2. GFS client translates request from (filename, data) -> (filename, chunk index), and sends it to master.
3. Master responds with chunk handle and (primary +secondary) replica location.
4. CLient pushes write data to all locations. Date is stored in chunkservers' internal buffers.
5. Client sends write command to primary.
6. Primary determines serial order for data instances stored in tis buffer and writes the instances in that order to the chunk.
7. Primary sends serial order to the secondaries and tells them to perform the write.
8. Secondaries respond to the primary.
9. Primary responds back to client.

*If write fails at one of chunkservers, client is informed and retries the write*

### Append Algorithm
1. APplication originates record append request.
2. GFS client translates request and sends it to master.
3. Master responds with chunk handle and (primary + secdonary) replica locations.
4. CLient pushes write data to all locations.
5. Primary checks if record fits in specified chunk.
6. If record does not fit, then the primary:
⋅⋅* pads the chunk,
⋅⋅* tells secondaries to do the same
⋅⋅* informs the client of such
⋅⋅* client then retries apend with next chunk
7. If record fits, then the primary:
⋅⋅* appends teh record
⋅⋅* tells secondariesi to do the same
⋅⋅* receives responses from secondaries
⋅⋅* sends final response to client