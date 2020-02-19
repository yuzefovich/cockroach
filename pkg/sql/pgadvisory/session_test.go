// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pgadvisory_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/sql/pgadvisory"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestSession(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	args := base.TestClusterArgs{}
	tc := testcluster.StartTestCluster(t, 1, args)
	defer tc.Stopper().Stop(ctx)
	s0 := tc.Server(0)
	db0 := s0.DB()

	log.Info(ctx, "Start test")
	fm := pgadvisory.NewFakeLockManager()
	fm.Start(ctx, tc.Stopper())
	session := pgadvisory.NewSession(db0, fm)

	txn1 := db0.NewTxn(ctx, "txn1")
	log.Info(ctx, "First AcquireEx")
	session.TxnLockSh(ctx, txn1, 1)
	session.TxnLockSh(ctx, txn1, 1)
	txn2 := db0.NewTxn(ctx, "txn2")
	session.TxnLockSh(ctx, txn2, 1)
	txn1.Commit(ctx)
	log.Info(ctx, "Second AcquireEx")
	session.TxnLockEx(ctx, txn2, 1)
	txn2.Commit(ctx)
}
