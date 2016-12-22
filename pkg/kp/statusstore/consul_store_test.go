package statusstore

import (
	"bytes"
	"testing"

	"github.com/square/p2/pkg/kp/consulutil"
)

func TestSetAndGetStatus(t *testing.T) {
	status := Status([]byte("some_status"))
	consulStore := storeWithFakeKV()

	// Set a status
	err := consulStore.SetStatus(PC, "some_id", "some_namespace", status)
	if err != nil {
		t.Fatalf("Unable to set status: %s", err)
	}

	// Confirm that we get the same thing back when we Get() it
	returnedStatus, _, err := consulStore.GetStatus(PC, "some_id", "some_namespace")
	if err != nil {
		t.Fatalf("Unable to get status: %s", err)
	}

	if !bytes.Equal(returnedStatus.Bytes(), status.Bytes()) {
		t.Errorf("Returned status bytes didn't match set status bytes: '%s' != '%s'", string(returnedStatus.Bytes()), string(status.Bytes()))
	}
}

func TestEmptyStringNotAllowed(t *testing.T) {
	status := Status([]byte("some_status"))
	consulStore := storeWithFakeKV()

	// Attempt to set status with empty resource type
	err := consulStore.SetStatus("", "some_id", "some_namespace", status)
	if err == nil {
		t.Error("Should have gotten an error using an empty resource type")
	}

	// Attempt to set status with empty id type
	err = consulStore.SetStatus(PC, "", "some_namespace", status)
	if err == nil {
		t.Error("Should have gotten an error using an empty resource ID")
	}

	// Attempt to set status with empty namespace
	err = consulStore.SetStatus(PC, "some_id", "", status)
	if err == nil {
		t.Error("Should have gotten an error using an empty namespace")
	}
}

func TestDeleteStatus(t *testing.T) {
	status := Status([]byte("some_status"))
	consulStore := storeWithFakeKV()

	// Set a status
	err := consulStore.SetStatus(PC, "some_id", "some_namespace", status)
	if err != nil {
		t.Fatalf("Unable to set status: %s", err)
	}

	// Confirm that the status was successfully set
	returnedStatus, _, err := consulStore.GetStatus(PC, "some_id", "some_namespace")
	if err != nil {
		t.Fatalf("Unable to get status: %s", err)
	}

	if !bytes.Equal(returnedStatus.Bytes(), status.Bytes()) {
		t.Errorf("Returned status bytes didn't match set status bytes: '%s' != '%s'", string(returnedStatus.Bytes()), string(status.Bytes()))
	}

	// Now delete it
	err = consulStore.DeleteStatus(PC, "some_id", "some_namespace")
	if err != nil {
		t.Fatalf("Unable to delete status: %s", err)
	}

	// Now try to get it and confirm we get the correct error type
	returnedStatus, _, err = consulStore.GetStatus(PC, "some_id", "some_namespace")
	if err == nil {
		t.Fatal("Expected an error getting a deleted status")
	}

	if !IsNoStatus(err) {
		t.Fatalf("Expected a noStatusError, but was %s", err)
	}
}

func TestGetAllStatusForResource(t *testing.T) {
	status := Status([]byte("some_status"))
	otherStatus := Status([]byte("some_other_status"))
	consulStore := storeWithFakeKV()

	// Set a status for one namespace
	err := consulStore.SetStatus(PC, "some_id", "some_namespace", status)
	if err != nil {
		t.Fatalf("Unable to set status: %s", err)
	}

	// Confirm that the status was successfully set
	returnedStatus, _, err := consulStore.GetStatus(PC, "some_id", "some_namespace")
	if err != nil {
		t.Fatalf("Unable to get status: %s", err)
	}

	if !bytes.Equal(returnedStatus.Bytes(), status.Bytes()) {
		t.Errorf("Returned status bytes didn't match set status bytes: '%s' != '%s'", string(returnedStatus.Bytes()), string(status.Bytes()))
	}

	// Set a status for another namespace
	err = consulStore.SetStatus(PC, "some_id", "some_other_namespace", otherStatus)
	if err != nil {
		t.Fatalf("Unable to set status: %s", err)
	}

	// Confirm that the status was successfully set
	returnedOtherStatus, _, err := consulStore.GetStatus(PC, "some_id", "some_other_namespace")
	if err != nil {
		t.Fatalf("Unable to get status: %s", err)
	}

	if !bytes.Equal(returnedOtherStatus.Bytes(), otherStatus.Bytes()) {
		t.Errorf("Returned status bytes didn't match set status bytes: '%s' != '%s'", string(returnedOtherStatus.Bytes()), string(otherStatus.Bytes()))
	}

	// Now fetch all statuses for that resource
	statusMap, err := consulStore.GetAllStatusForResource(PC, "some_id")
	if err != nil {
		t.Fatalf("Unable to fetch all status for resource: %s", err)
	}

	returnedStatus = statusMap["some_namespace"]
	if !bytes.Equal(returnedStatus, status.Bytes()) {
		t.Errorf("Returned status bytes didn't match set status bytes: '%s' != '%s'", string(returnedStatus.Bytes()), string(status.Bytes()))
	}

	returnedOtherStatus = statusMap["some_other_namespace"]
	if !bytes.Equal(returnedOtherStatus, otherStatus.Bytes()) {
		t.Errorf("Returned status bytes didn't match set status bytes: '%s' != '%s'", string(returnedOtherStatus.Bytes()), string(otherStatus.Bytes()))
	}
}

func TestGetAllStatusForResourceType(t *testing.T) {
	status := Status([]byte("some_status"))
	otherStatus := Status([]byte("some_other_status"))
	otherPCStatus := Status([]byte("some_other_pc_status"))
	consulStore := storeWithFakeKV()

	// Set a status for one namespace
	err := consulStore.SetStatus(PC, "some_id", "some_namespace", status)
	if err != nil {
		t.Fatalf("Unable to set status: %s", err)
	}

	// Confirm that the status was successfully set
	returnedStatus, _, err := consulStore.GetStatus(PC, "some_id", "some_namespace")
	if err != nil {
		t.Fatalf("Unable to get status: %s", err)
	}

	if !bytes.Equal(returnedStatus.Bytes(), status.Bytes()) {
		t.Errorf("Returned status bytes didn't match set status bytes: '%s' != '%s'", string(returnedStatus.Bytes()), string(status.Bytes()))
	}

	// Set a status for another namespace
	err = consulStore.SetStatus(PC, "some_id", "some_other_namespace", otherStatus)
	if err != nil {
		t.Fatalf("Unable to set status: %s", err)
	}

	returnedOtherStatus, _, err := consulStore.GetStatus(PC, "some_id", "some_other_namespace")
	if err != nil {
		t.Fatalf("Unable to get status: %s", err)
	}

	if !bytes.Equal(returnedOtherStatus.Bytes(), otherStatus.Bytes()) {
		t.Errorf("Returned status bytes didn't match set status bytes: '%s' != '%s'", string(returnedOtherStatus.Bytes()), string(otherStatus.Bytes()))
	}

	// Set a status for another PC
	err = consulStore.SetStatus(PC, "some_other_id", "some_namespace", otherPCStatus)
	if err != nil {
		t.Fatalf("Unable to set status: %s", err)
	}

	// Confirm that the status was successfully set
	returnedOtherPCStatus, _, err := consulStore.GetStatus(PC, "some_other_id", "some_namespace")
	if err != nil {
		t.Fatalf("Unable to get status: %s", err)
	}

	if !bytes.Equal(returnedOtherPCStatus.Bytes(), otherPCStatus.Bytes()) {
		t.Errorf("Returned status bytes didn't match set status bytes: '%s' != '%s'", string(returnedOtherPCStatus.Bytes()), string(otherPCStatus.Bytes()))
	}

	// Now fetch all status for the PC resource type and make sure we get our 3 statuses back
	allStatusForPC, err := consulStore.GetAllStatusForResourceType(PC)
	if err != nil {
		t.Fatalf("Unable to fetch all status for PC type: %s", err)
	}

	firstPCStatus, ok := allStatusForPC["some_id"]
	if !ok {
		t.Fatal("No status returned for first pod cluster")
	}

	secondPCStatus, ok := allStatusForPC["some_other_id"]
	if !ok {
		t.Fatal("No status returned for second pod cluster")
	}

	returnedStatus = firstPCStatus["some_namespace"]
	returnedOtherStatus = firstPCStatus["some_other_namespace"]
	returnedOtherPCStatus = secondPCStatus["some_namespace"]

	if !bytes.Equal(returnedStatus.Bytes(), status.Bytes()) {
		t.Errorf("Returned status bytes didn't match set status bytes: '%s' != '%s'", string(returnedStatus.Bytes()), string(status.Bytes()))
	}

	if !bytes.Equal(returnedOtherStatus.Bytes(), otherStatus.Bytes()) {
		t.Errorf("Returned status bytes didn't match set status bytes: '%s' != '%s'", string(returnedOtherStatus.Bytes()), string(otherStatus.Bytes()))
	}

	if !bytes.Equal(returnedOtherPCStatus.Bytes(), otherPCStatus.Bytes()) {
		t.Errorf("Returned status bytes didn't match set status bytes: '%s' != '%s'", string(returnedOtherPCStatus.Bytes()), string(otherPCStatus.Bytes()))
	}
}

func storeWithFakeKV() *consulStore {
	return &consulStore{
		kv: consulutil.NewFakeClient().KV(),
	}
}
