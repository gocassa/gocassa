package gocassa

type RowNotFoundError struct {
	err string
}

func (r RowNotFoundError) Error() string {
	return r.err
}
