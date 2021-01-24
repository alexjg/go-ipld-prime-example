# `go-ipld-prime` Example

This is a very simple example of how to use `go-ipld-prime` to read and write IPLD data. It creates a linked list of "events".


## Usage

It is assumed that the IPFS HTTP API is running on `localhost:5000`

### Pushing an event

To create an event with no predecessors

```bash
./ipld-prime-demo push '<event payload>'
```

This will print the CID of the resulting event

To create an event with a predecessor:

```bash
./ipld-prime-demo push '<event payload>' <predecessor payload CID>
```

### Fetching events

```bash
./ipld-prime-demo fetch <head event CID>
```

This will print all the events which are reachable from `<head event CID>`
