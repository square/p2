package pp

type OwnerApp struct {
	Email string
	Users []string
}

func FetchOwners(name string) (OwnerApp, error) {
	// TODO: Actually do something
	spec := OwnerApp{
		Email: "noreply@example.com",
		Users: []string{"xavier"},
	}
	return spec, nil
}
