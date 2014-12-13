module jack.Tests

open NUnit.Framework
open FsUnit

[<Test>]
let ``Commands.put should insert a job into the queue and Commands.reserve should retrieve a job from the queue`` () = 
  let conn = Connection.parseAddress "localhost:11300" |> Connection.connect
  let tubeName = "testTube"
  let commandToPut = "testcommand1"

  let useR = Commands.useTube conn tubeName
//  printfn "useR = %A" useR
  match useR with
  | Success (Using name) ->
    name |> should equal tubeName
  | _ -> failwith "use command failed"

  let putR = Commands.put conn 1 0 30 commandToPut
//  printfn "putR = %A" putR
  (match putR with Success (PutInserted id) -> true | _ -> false) |> should be True

  let watchR = Commands.watch conn tubeName
//  printfn "watchR = %A" watchR
  match watchR with 
  | Success (Watching count) -> count |> should equal 2   // default and testTube
  | _ -> "watch command blew up" |> should not' (be True)

  let reserveR = Commands.reserveWithTimeout conn 5
//  printfn "reserveR = %A" reserveR
  (match reserveR with Success (Reserved (id, bytes, payload)) -> true | _ -> false) |> should be True

  match (putR, reserveR) with
  | Success (PutInserted putId), Success (Reserved (reservedId, bytes, payload)) ->
    reservedId |> should equal putId
    payload |> should equal commandToPut
  | _ -> failwith "reserved job is not the same as put job"

  let id = match putR with Success (PutInserted putId) -> putId | _ -> 0L
  let deleteR = Commands.delete conn id
  (match deleteR with Success Deleted -> true | _ -> false) |> should be True

[<Test>]
let ``Client should be nicer to use than Commands`` () = 
  let client = Connection.parseAddress "localhost:11300" |> Client.connect
  let tubeName = "testTube"
  let commandToPut = "testcommand1"

  let useR = client.useTube tubeName
  match useR with
  | Success name ->
    name |> should equal tubeName
  | _ -> failwith "use command failed"

  let putR = client.put 1 0 30 commandToPut
  (match putR with Success _ -> true | _ -> false) |> should be True

  let watchR = client.watch tubeName
  match watchR with 
  | Success count -> count |> should equal 2   // default and testTube
  | _ -> "watch command blew up" |> should not' (be True)

  let reserveR = client.reserveWithTimeout 5
  (match reserveR with Success _ -> true | _ -> false) |> should be True

  match (putR, reserveR) with
  | Success putId, Success (reservedId, payload) ->
    reservedId |> should equal putId
    payload |> should equal commandToPut
  | _ -> failwith "reserved job is not the same as put job"

  let id = match putR with Success putId -> putId | _ -> 0L
  let deleteR = client.delete id
  (match deleteR with Success _ -> true | _ -> false) |> should be True
