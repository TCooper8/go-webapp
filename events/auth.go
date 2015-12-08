package events_auth

type Register struct {
  FirstName string
  LastName  string
  Email     string
  Password  string
  Birthday  string
  Gender    string
}

type Login struct {
  Email     string
  Password  string
}
