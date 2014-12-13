namespace jack

open System
open System.Text
open System.Net.Sockets

open FParsec

// Result type and module from http://fsharpforfunandprofit.com/posts/recipe-part2/
// the two-track type
type Result<'TSuccess,'TFailure> = 
  | Success of 'TSuccess
  | Failure of 'TFailure

module Result =
  // convert a single value into a two-track result
  let succeed x = 
    Success x

  // convert a single value into a two-track result
  let fail x = 
    Failure x

  // apply either a success function or failure function
  let either successFunc failureFunc twoTrackInput =
    match twoTrackInput with
    | Success s -> successFunc s
    | Failure f -> failureFunc f

  // convert a switch function into a two-track function
  let bind f = 
    either f fail

  // pipe a two-track value into a switch function 
  let (>>=) x f = 
    bind f x

  // compose two switches into another switch
  let (>=>) s1 s2 = 
    s1 >> bind s2

  // convert a one-track function into a switch
  let switch f = 
    f >> succeed

  // convert a one-track function into a two-track function
  let map f = 
    either (f >> succeed) fail

  // convert a dead-end function into a one-track function
  let tee f x = f x; x 

  // convert a one-track function into a switch with exception handling
  let tryCatch f exnHandler x =
    try
      f x |> succeed
    with
    | ex -> exnHandler ex |> fail

  // convert two one-track functions into a two-track function
  let doubleMap successFunc failureFunc =
    either (successFunc >> succeed) (failureFunc >> fail)

  // add two switches in parallel
  let plus addSuccess addFailure switch1 switch2 x = 
    match (switch1 x),(switch2 x) with
    | Success s1,Success s2 -> Success (addSuccess s1 s2)
    | Failure f1,Success _  -> Failure f1
    | Success _ ,Failure f2 -> Failure f2
    | Failure f1,Failure f2 -> Failure (addFailure f1 f2)


type JobId = int64
type TubeName = string
type Job = JobId * string
type TubeCount = int64
type KickedJobCount = int64
type YamlEncodedString = string

type Configuration = {
  putPriority: int
  putDelay: int
  putTtr: int         // ttr = time to run
  beanstalkdUrl: string
}

//type Command =
//  // Producer Commands
//  Put of priority: int * 
//         delay: int * 
//         ttr: int * 
//         payload: string
//  | Use of name: string
//  // Worker Commands
//  | Reserve
//  | ReserveWithTimeout of seconds: int
//  | Delete of id: int64
//  | Release of id: int64 * 
//               priority: int *
//               delay: int
//  | Bury of id: int64 *
//            priority: int
//  | Touch of id: int64
//  | Watch of name: string
//  | Ignore of name: string
//  // Other Commands
//  | Peek of id: int64
//  | PeekReady
//  | PeekDelayed
//  | PeekBuried
//  | Kick of bound: int
//  | KickJob of id: int64
//  | StatsJob of id: int64
//  | StatsTube of id: int64
//  | Stats
//  | ListTubes
//  | ListTubeUsed
//  | ListTubeWatched
//  | Quit
//  | PauseTube of name: string *
//                 delay: int64

type Response =
  // Error responses
  OutOfMemory
  | InternalError
  | BadFormat
  | UnknownCommand
  // Put responses
  | PutInserted of id: JobId
  | PutBuried of id: JobId
  | PutExpectedCrLf
  | PutJobTooBig
  | PutDraining
  // Use responses
  | Using of name: string
  // Reserve and ReserveWithTimeout responses
  | DeadlineSoon
  | TimedOut
  | Reserved of id: JobId *
                bytes: int64 *
                payload: string
  // Delete responses
  | Deleted
  | NotFound
  // Release responses
  | Released
  | Buried
  // | NotFound, but it is already defined
  // Bury responses
  // | Buried, but it is already defined
  // | NotFound, but it is already defined
  // Touch responses
  | Touched
  // | NotFound, but it is already defined
  // Watch responses
  | Watching of count: int64
  // Ignore responses
  // | Watching of count: int, but it is already defined
  | NotIgnored
  // Peek, PeekReady, PeekDelayed, PeekBuried responses
  // | NotFound, but it is already defined
  | Found of id: JobId *
             bytes: int64 *
             payload: string
  // Kick responses
  | Kicked of count: int64
  // KickJob responses
  // | Kicked of count: int64, but it is already defined
  // StatsJob responses
  // | NotFound, but it is already defined
  | Ok of bytes: int64 *
          payload: string
  // StatsTube responses
  // | NotFound, but it is already defined
  // | Ok of payload: string, but it is already defined
  // Stats responses
  // | Ok of payload: string, but it is already defined
  // ListTubes responses
  // | Ok of payload: string, but it is already defined
  // ListTubeUsed responses:
  // | Using of name: string, but it is already defined
  // ListTubesWatched responses
  // | Ok of payload: string, but it is already defined
  // PauseTube responses
  | Paused
  // | NotFound, but it is already defined

type JobStatus = ReadyStatus | ReservedStatus | DelayedStatus | BuriedStatus

module Array =
  let sample (xs: array<'t>): 't =
    let rnd = new Random(int DateTime.Now.Ticks)
    xs.[rnd.Next(0, xs.Length)]

[<CompilationRepresentationAttribute(CompilationRepresentationFlags.ModuleSuffix)>]
module Configuration =
  let DefaultPort = 11300

  let build putPriority putDelay putTtr beanstalkdUrl: Configuration = 
    {
      putPriority = putPriority
      putDelay = putDelay
      putTtr = putTtr
      beanstalkdUrl = beanstalkdUrl
    }

module Connection =
  type Address = string * int

  let parseAddress (addressString: string): Address =
    let parts = addressString.Split([| ':' |])
    match parts with
    | [| host |] -> (host, Configuration.DefaultPort)
    | [| host; port |] -> (host, int port)
    | _ -> failwith <| sprintf "Unable to parse address string: %s" addressString

  type Connection = {
    host: string
    port: int
    tcpClient: TcpClient
    stream: NetworkStream
  }

  let connect (address: Address): Connection = 
    let host, port = address
    let client = new TcpClient(host, port)
//    client.ReceiveTimeout <- 5000   // client.GetStream().Read() should block for at most 5 seconds
    {
      host = host
      port = port
      tcpClient = client
      stream = client.GetStream()
    }

  let disconnect connection: unit =
    let stream = connection.stream
    let client = connection.tcpClient
    stream.Close()
    client.Close()

  let read connection: string =
    let stream = connection.stream
    let buffer = Array.zeroCreate<byte> 1024
    let mutable strBuffer = ""
    let strBuilder = new StringBuilder()
    let mutable numberOfBytesRead = 0

    // Incoming message may be larger than the buffer size. 
    numberOfBytesRead <- stream.Read(buffer, 0, buffer.Length)
    strBuffer <- Encoding.ASCII.GetString(buffer, 0, numberOfBytesRead)
//    printfn "strBuffer = %s" strBuffer
    strBuilder.AppendFormat("{0}", strBuffer) |> ignore

    while stream.DataAvailable do
      numberOfBytesRead <- stream.Read(buffer, 0, buffer.Length)
      strBuffer <- Encoding.ASCII.GetString(buffer, 0, numberOfBytesRead)
//      printfn "strBuffer = %s" strBuffer
      strBuilder.AppendFormat("{0}", strBuffer) |> ignore
    
    strBuilder.ToString()

  let write (str: string) connection: unit =
    let buffer = Encoding.ASCII.GetBytes(str)
    let stream = connection.stream
    stream.Write(buffer, 0, buffer.Length)


// the parsers parse the responses as defined in the protocol document: https://github.com/kr/beanstalkd/blob/master/doc/protocol.md
module ResponseParser =
  let parse<'Result> (parser: Parser<'Result, unit>) (inputString: string): Result<'Result, string> =
    match run parser inputString with
    | ParserResult.Success(result, _, _) -> Success result
    | ParserResult.Failure(errorMsg, _, _) -> Failure errorMsg

  let parseResponse = parse<Response>

  let nameParser = (asciiLetter <|> digit <|> anyOf "+/;.$_()") .>>. manyChars (asciiLetter <|> digit <|> anyOf "-+/;.$_()") |>> (fun (firstLetter, remainingLetters) -> firstLetter.ToString() + remainingLetters)
  let parseName = parse nameParser

  let space = pchar ' '

  let outOfMemoryParser = pstring "OUT_OF_MEMORY" .>> newline >>% OutOfMemory
  let internalErrorParser = pstring "INTERNAL_ERROR" .>> newline >>% InternalError
  let badFormatParser = pstring "BAD_FORMAT" .>> newline >>% BadFormat
  let unknownCommandParser = pstring "UNKNOWN_COMMAND" .>> newline >>% UnknownCommand
  let errorParser = outOfMemoryParser <|> internalErrorParser <|> badFormatParser <|> unknownCommandParser

  let putParser =
    errorParser
    <|> (pstring "INSERTED" >>. space >>. pint64 .>> newline |>> PutInserted)
    <|> (pstring "BURIED" >>. space >>. pint64 .>> newline |>> PutBuried)
    <|> (pstring "EXPECTED_CRLF" .>> newline >>% PutExpectedCrLf)
    <|> (pstring "JOB_TOO_BIG" .>> newline >>% PutJobTooBig)
    <|> (pstring "DRAINING" .>> newline >>% PutDraining)
  let parsePut = parseResponse putParser

  let notFoundParser = pstring "NOT_FOUND" .>> newline >>% NotFound
  let buriedParser = pstring "BURIED" .>> newline >>% Buried
  let watchingParser = pstring "WATCHING" >>. space >>. pint64 .>> newline |>> Watching
  let okParser = pstring "OK" >>. space >>. tuple2 (pint64 .>> newline) (manyCharsTill anyChar (followedBy (newline .>> eof))) |>> Ok
  let usingParser = pstring "USING" >>. space >>. nameParser .>> newline |>> Using

  let useParser = 
    errorParser
    <|> usingParser
  let parseUse = parseResponse useParser

  let reserveParser =
    errorParser
    <|> (pstring "DEADLINE_SOON" .>> newline >>% DeadlineSoon)
    <|> (pstring "TIMED_OUT" .>> newline >>% TimedOut)
    <|> (pstring "RESERVED" >>. space >>. tuple3 (pint64 .>> space) (pint64 .>> newline) (manyCharsTill anyChar (followedBy (newline .>> eof))) |>> Reserved)
  let parseReserve = parseResponse reserveParser

  let deleteParser =
    errorParser
    <|> (pstring "DELETED" .>> newline >>% Deleted)
    <|> notFoundParser
  let parseDelete = parseResponse deleteParser

  let releaseParser =
    errorParser
    <|> (pstring "RELEASED" .>> newline >>% Released)
    <|> buriedParser
    <|> notFoundParser
  let parseRelease = parseResponse releaseParser

  let buryParser = 
    errorParser
    <|> buriedParser 
    <|> notFoundParser
  let parseBury = parseResponse buryParser

  let touchParser = 
    errorParser
    <|> (pstring "TOUCHED" .>> newline >>% Touched)
    <|> notFoundParser
  let parseTouch = parseResponse touchParser

  let watchParser = 
    errorParser
    <|> watchingParser
  let parseWatch = parseResponse watchParser

  let ignoreParser = 
    errorParser
    <|> watchingParser
    <|> (pstring "NOT_IGNORED" .>> newline >>% NotIgnored)
  let parseIgnore = parseResponse ignoreParser

  let peekParser =
    errorParser
    <|> notFoundParser
    <|> (pstring "FOUND" >>. space >>. tuple3 (pint64 .>> space) (pint64 .>> newline) (manyCharsTill anyChar (followedBy (newline .>> eof))) |>> Found)
  let parsePeek = parseResponse peekParser

  let kickParser = 
    errorParser
    <|> (pstring "KICKED" >>. space >>. pint64 .>> newline |>> Kicked)
  let parseKick = parseResponse kickParser

  let kickJobParser = 
    errorParser
    <|> notFoundParser
    <|> (pstring "KICKED" .>> newline >>% Kicked 1L)
  let parseKickJob = parseResponse kickJobParser

  let statsJobParser = 
    errorParser
    <|> notFoundParser 
    <|> okParser
  let parseStatsJob = parseResponse statsJobParser

  let statsTubeParser = 
    errorParser
    <|> notFoundParser 
    <|> okParser
  let parseStatsTube = parseResponse statsTubeParser

  let statsParser = 
    errorParser
    <|> okParser
  let parseStats = parseResponse statsParser

  let listTubesParser = 
    errorParser
    <|> okParser
  let parseListTubes = parseResponse listTubesParser

  let listTubeUsedParser = 
    errorParser
    <|> usingParser
  let parseListTubeUsed = parseResponse listTubeUsedParser

  let listTubesWatchedParser = 
    errorParser
    <|> okParser
  let parseListTubesWatched = parseResponse listTubesWatchedParser

  let pauseTubeParser = 
    errorParser
    <|> (pstring "PAUSED" .>> newline >>% Paused)
    <|> notFoundParser
  let parsePauseTube = parseResponse pauseTubeParser

module Commands =
  let rec safeWrite command connection remainingRetryCount: Result<unit, string> =
    if remainingRetryCount = 0 then
      Failure <| sprintf "Unable to write command %s" command
    else
      try
//        printfn "safeWrite %s" command
        Success <| Connection.write command connection
      with
        | :? IO.IOException          -> safeWrite command connection <| remainingRetryCount - 1
        | :? ObjectDisposedException -> safeWrite command connection <| remainingRetryCount - 1

  let rec safeRead command parseResponseFn connection remainingRetryCount: Result<Response, string> =
    if remainingRetryCount = 0 then
      Failure <| sprintf "Unable to read response from command %s" command
    else
      try
        let response = Connection.read connection
//          printfn "safeRead = %s" response
        response |> parseResponseFn
      with
        | :? IO.IOException          -> safeRead command parseResponseFn connection <| remainingRetryCount - 1
        | :? ObjectDisposedException -> safeRead command parseResponseFn connection <| remainingRetryCount - 1

  let safeTransmit command parseResponseFn connection remainingRetryCount: Result<Response, string> =
    safeWrite command connection remainingRetryCount |> ignore
    safeRead command parseResponseFn connection remainingRetryCount

  let transmit command parseResponseFn connection: Result<Response, string> =
    safeTransmit command parseResponseFn connection 3

  let write command connection: Result<unit, string> =
    safeWrite command connection 3


  let put connection priority delay ttr (payload: string): Result<Response, string> =
    let command = sprintf "put %i %i %i %i\r\n%s\r\n" priority delay ttr payload.Length payload
    transmit command ResponseParser.parsePut connection

  let useTube connection tubeName: Result<Response, string> =
    let command = sprintf "use %s\r\n" tubeName
    transmit command ResponseParser.parseUse connection

  let reserve connection: Result<Response, string> =
    let command = "reserve\r\n"
    transmit command ResponseParser.parseReserve connection

  let reserveWithTimeout connection timeoutInSeconds: Result<Response, string> =
    let command = sprintf "reserve-with-timeout %i\r\n" timeoutInSeconds
    transmit command ResponseParser.parseReserve connection
  
  let delete connection id: Result<Response, string> =
    let command = sprintf "delete %i\r\n" id
    transmit command ResponseParser.parseDelete connection

  let release connection id priority delay: Result<Response, string> =
    let command = sprintf "release %i %i %i\r\n" id priority delay
    transmit command ResponseParser.parseRelease connection

  let bury connection id priority: Result<Response, string> =
    let command = sprintf "bury %i %i\r\n" id priority
    transmit command ResponseParser.parseBury connection

  let touch connection id: Result<Response, string> =
    let command = sprintf "touch %i\r\n" id
    transmit command ResponseParser.parseTouch connection

  let watch connection tubeName: Result<Response, string> =
    let command = sprintf "watch %s\r\n" tubeName
    transmit command ResponseParser.parseWatch connection

  let ignore connection tubeName: Result<Response, string> =
    let command = sprintf "ignore %sr\n" tubeName
    transmit command ResponseParser.parseIgnore connection

  let peek connection id: Result<Response, string> =
    let command = sprintf "peek %i\r\n" id
    transmit command ResponseParser.parsePeek connection

  let peekReady connection: Result<Response, string> =
    let command = "peek-ready\r\n"
    transmit command ResponseParser.parsePeek connection

  let peekDelayed connection: Result<Response, string> =
    let command = "peek-delayed\r\n"
    transmit command ResponseParser.parsePeek connection

  let peekBuried connection: Result<Response, string> =
    let command = "peek-buried\r\n"
    transmit command ResponseParser.parsePeek connection

  let kick connection bound: Result<Response, string> =
    let command = sprintf "kick %i\r\n" bound
    transmit command ResponseParser.parseKick connection

  let kickJob connection id: Result<Response, string> =
    let command = sprintf "kick-job %i\r\n" id
    transmit command ResponseParser.parseKickJob connection

  let statsJob connection id: Result<Response, string> =
    let command = sprintf "stats-job %i\r\n" id
    transmit command ResponseParser.parseStatsJob connection

  let statsTube connection tubeName: Result<Response, string> =
    let command = sprintf "stats-tube %s\r\n" tubeName
    transmit command ResponseParser.parseStatsTube connection

  let stats connection: Result<Response, string> =
    let command = "stats\r\n"
    transmit command ResponseParser.parseStats connection

  let listTubes connection: Result<Response, string> =
    let command = "list-tubes\r\n"
    transmit command ResponseParser.parseListTubes connection

  let listTubeUsed connection: Result<Response, string> =
    let command = "list-tube-used\r\n"
    transmit command ResponseParser.parseListTubeUsed connection

  let listTubesWatched connection: Result<Response, string> =
    let command = "list-tubes-watched\r\n"
    transmit command ResponseParser.parseListTubesWatched connection

  let quit connection: Result<unit, string> =
    let command = "quit\r\n"
    write command connection

  let pauseTube connection tubeName delay: Result<Response, string> =
    let command = sprintf "pause-tube %s %i\r\n" tubeName delay
    transmit command ResponseParser.parsePauseTube connection


module Client =
  let put connection priority delay ttr (payload: string): Result<JobId, string> =
    let result = Commands.put connection priority delay ttr payload
    match result with
    | Success (PutInserted id) -> Success id
    | Success (PutBuried id) -> Failure <| sprintf "New job id=%i buried; Server out of memory." id
    | Success PutExpectedCrLf -> Failure "The submitted job was malformed. Job body must be followed by CRLF."
    | Success PutJobTooBig -> Failure "The submitted job was too big. Job body must not be larger than max-job-size bytes."
    | Success PutDraining -> Failure "Server is in drain mode and no longer accepting new jobs."
    | Failure msg -> Failure msg
    | _ -> Failure "Unknown error."

  let useTube connection tubeName: Result<TubeName, string> =
    let result = Commands.useTube connection tubeName
    match result with
    | Success (Using name) -> Success name
    | Failure msg -> Failure msg
    | _ -> Failure "Unknown error."

  let reserve connection: Result<Job, string> =
    let result = Commands.reserve connection
    match result with
    | Success (Reserved (id, bytes, payload)) -> Success (id, payload)
    | Success DeadlineSoon -> Failure "The current connection has already been issued a job whose TTR is about to expire. Release or delete the currently reserved job, or the server will automatically release it."
    | Success TimedOut -> Failure "Timed out waiting for a job to become available."
    | Failure msg -> Failure msg
    | _ -> Failure "Unknown error."


  let reserveWithTimeout connection timeoutInSeconds: Result<Job, string> =
    let result = Commands.reserveWithTimeout connection timeoutInSeconds
    match result with
    | Success (Reserved (id, bytes, payload)) -> Success (id, payload)
    | Success DeadlineSoon -> Failure "The current connection has already been issued a job whose TTR is about to expire. Release or delete the currently reserved job, or the server will automatically release it."
    | Success TimedOut -> Failure "Timed out waiting for a job to become available."
    | Failure msg -> Failure msg
    | _ -> Failure "Unknown error."

  let delete connection id: Result<unit, string> =
    let result = Commands.delete connection id
    match result with
    | Success Deleted -> Success ()
    | Success NotFound -> Failure <| sprintf "Job %i does not exist or is not either reserved by the client, ready, or buried. This could happen if the job timed out before the client sent the delete command." id
    | Failure msg -> Failure msg
    | _ -> Failure "Unknown error."

  let release connection id priority delay: Result<unit, string> =
    let result = Commands.release connection id priority delay
    match result with
    | Success Released -> Success ()
    | Success Buried -> Failure <| sprintf "Job %i buried; Server out of memory." id
    | Success NotFound -> Failure "Job does not exist or is not reserved by the client."
    | Failure msg -> Failure msg
    | _ -> Failure "Unknown error."

  let bury connection id priority: Result<unit, string> =
    let result = Commands.bury connection id priority
    match result with
    | Success Buried -> Success ()
    | Success NotFound -> Failure "Job does not exist or is not reserved by the client."
    | Failure msg -> Failure msg
    | _ -> Failure "Unknown error."

  let touch connection id: Result<unit, string> =
    let result = Commands.touch connection id
    match result with
    | Success Touched -> Success ()
    | Success NotFound -> Failure "Job does not exist or is not reserved by the client."
    | Failure msg -> Failure msg
    | _ -> Failure "Unknown error."

  let watch connection tubeName: Result<TubeCount, string> =
    let result = Commands.watch connection tubeName
    match result with
    | Success (Watching count) -> Success count
    | Failure msg -> Failure msg
    | _ -> Failure "Unknown error."

  let ignore connection tubeName: Result<TubeCount, string> =
    let result = Commands.ignore connection tubeName
    match result with
    | Success (Watching count) -> Success count
    | Success NotIgnored -> Failure "Client attempted to ignore the only tube in its watch list. At least one tube must be watched."
    | Failure msg -> Failure msg
    | _ -> Failure "Unknown error."

  let peek connection id: Result<Job, string> =
    let result = Commands.peek connection id
    match result with
    | Success (Found (id, bytes, payload)) -> Success (id, payload)
    | Success NotFound -> Failure <|sprintf "Job %i does not exist." id
    | Failure msg -> Failure msg
    | _ -> Failure "Unknown error."

  let peekReady connection: Result<Job, string> =
    let result = Commands.peekReady connection
    match result with
    | Success (Found (id, bytes, payload)) -> Success (id, payload)
    | Success NotFound -> Failure "There are no jobs in the ready state."
    | Failure msg -> Failure msg
    | _ -> Failure "Unknown error."

  let peekDelayed connection: Result<Job, string> =
    let result = Commands.peekDelayed connection
    match result with
    | Success (Found (id, bytes, payload)) -> Success (id, payload)
    | Success NotFound -> Failure "There are no jobs in the delayed state."
    | Failure msg -> Failure msg
    | _ -> Failure "Unknown error."

  let peekBuried connection: Result<Job, string> =
    let result = Commands.peekBuried connection
    match result with
    | Success (Found (id, bytes, payload)) -> Success (id, payload)
    | Success NotFound -> Failure "There are no jobs in the buried state."
    | Failure msg -> Failure msg
    | _ -> Failure "Unknown error."

  let kick connection bound: Result<KickedJobCount, string> =
    let result = Commands.kick connection bound
    match result with
    | Success (Kicked count) -> Success count
    | Failure msg -> Failure msg
    | _ -> Failure "Unknown error."

  let kickJob connection id: Result<unit, string> =
    let result = Commands.kickJob connection id
    match result with
    | Success (Kicked 1L) -> Success ()
    | Success NotFound -> Failure <| sprintf "Job %i does not exist or is not in a kickable state." id
    | Failure msg -> Failure msg
    | _ -> Failure "Unknown error."

  let statsJob connection id: Result<YamlEncodedString, string> =
    let result = Commands.statsJob connection id
    match result with
    | Success (Ok (bytes, payload)) -> Success payload
    | Success NotFound -> Failure <| sprintf "Job %i does not exist." id
    | Failure msg -> Failure msg
    | _ -> Failure "Unknown error."

  let statsTube connection tubeName: Result<YamlEncodedString, string> =
    let result = Commands.statsTube connection tubeName
    match result with
    | Success (Ok (bytes, payload)) -> Success payload
    | Success NotFound -> Failure <| sprintf "Tube '%s' does not exist." tubeName
    | Failure msg -> Failure msg
    | _ -> Failure "Unknown error."

  let stats connection: Result<YamlEncodedString, string> =
    let result = Commands.stats connection
    match result with
    | Success (Ok (bytes, payload)) -> Success payload
    | Failure msg -> Failure msg
    | _ -> Failure "Unknown error."

  let listTubes connection: Result<YamlEncodedString, string> =
    let result = Commands.listTubes connection
    match result with
    | Success (Ok (bytes, payload)) -> Success payload
    | Failure msg -> Failure msg
    | _ -> Failure "Unknown error."

  let listTubeUsed connection: Result<TubeName, string> =
    let result = Commands.listTubeUsed connection
    match result with
    | Success (Using tubeName) -> Success tubeName
    | Failure msg -> Failure msg
    | _ -> Failure "Unknown error."

  let listTubesWatched connection: Result<YamlEncodedString, string> =
    let result = Commands.listTubes connection
    match result with
    | Success (Ok (bytes, payload)) -> Success payload
    | Failure msg -> Failure msg
    | _ -> Failure "Unknown error."

  let quit connection: Result<unit, string> = Commands.quit connection

  let pauseTube connection tubeName delay: Result<unit, string> =
    let result = Commands.pauseTube connection tubeName delay
    match result with
    | Success Paused -> Success ()
    | Success NotFound -> Failure <| sprintf "Tube '%s' does not exist." tubeName
    | Failure msg -> Failure msg
    | _ -> Failure "Unknown error."


  type ConnectedClient = {
    connection: Connection.Connection

    //  let put priority delay ttr (payload: string): Result<Id, string> =
    put: int -> int -> int -> string -> Result<JobId, string>

    //  let useTube tubeName: Result<Response, string> =
    useTube: string -> Result<TubeName, string>

    //  let reserve: Result<Job, string> =
    reserve: unit -> Result<Job, string>

    //  let reserveWithTimeout timeoutInSeconds: Result<Job, string> =
    reserveWithTimeout: int -> Result<Job, string>

    //  let delete id: Result<unit, string> =
    delete: JobId -> Result<unit, string>

    //  let release id priority delay: Result<unit, string> =
    release: JobId -> int -> int -> Result<unit, string>

    //  let bury id priority: Result<unit, string> =
    bury: JobId -> int -> Result<unit, string>

    //  let touch id: Result<unit, string> =
    touch: JobId -> Result<unit, string>

    //  let watch tubeName: Result<TubeCount, string> =
    watch: string -> Result<TubeCount, string>

    //  let ignore tubeName: Result<TubeCount, string> =
    ignore: string -> Result<TubeCount, string>

    //  let peek id: Result<Job, string> =
    peek: JobId -> Result<Job, string>

    //  let peekReady: Result<Job, string> =
    peekReady: unit -> Result<Job, string>

    //  let peekDelayed: Result<Job, string> =
    peekDelayed: unit -> Result<Job, string>

    //  let peekBuried: Result<Job, string> =
    peekBuried: unit -> Result<Job, string>

    //  let kick bound: Result<KickedJobCount, string> =
    kick: int -> Result<KickedJobCount, string>

    //  let kickJob id: Result<unit, string> =
    kickJob: JobId -> Result<unit, string>

    //  let statsJob id: Result<YamlEncodedString, string> =
    statsJob: JobId -> Result<YamlEncodedString, string>

    //  let statsTube tubeName: Result<YamlEncodedString, string> =
    statsTube: string -> Result<YamlEncodedString, string>

    //  let stats: Result<YamlEncodedString, string> =
    stats: unit -> Result<YamlEncodedString, string>

    //  let listTubes: Result<YamlEncodedString, string> =
    listTubes: unit -> Result<YamlEncodedString, string>

    //  let listTubeUsed: Result<TubeName, string> =
    listTubeUsed: unit -> Result<TubeName, string>

    //  let listTubesWatched: Result<YamlEncodedString, string> =
    listTubesWatched: unit -> Result<YamlEncodedString, string>

    //  let quit: Result<unit, string> =
    quit: unit -> Result<unit, string>

    //  let pauseTube tubeName delay: Result<unit, string> =
    pauseTube: string -> int -> Result<unit, string>
  }

  let connect (address: Connection.Address): ConnectedClient =
    let connection = Connection.connect address
    {
      connection = connection
      put = put connection
      useTube = useTube connection
      reserve = fun () -> reserve connection
      reserveWithTimeout = reserveWithTimeout connection
      delete = delete connection
      release = release connection
      bury = bury connection
      touch = touch connection
      watch = watch connection
      ignore = ignore connection
      peek = peek connection
      peekReady = fun () -> peekReady connection
      peekDelayed = fun () -> peekDelayed connection
      peekBuried = fun () -> peekBuried connection
      kick = kick connection
      kickJob = kickJob connection
      statsJob = statsJob connection
      statsTube = statsTube connection
      stats = fun () -> stats connection
      listTubes = fun () -> listTubes connection
      listTubeUsed = fun () -> listTubeUsed connection
      listTubesWatched = fun () -> listTubesWatched connection
      quit = fun () -> quit connection
      pauseTube = pauseTube connection
    }

  let disconnect (client: ConnectedClient): unit =
    Connection.disconnect client.connection

(**)