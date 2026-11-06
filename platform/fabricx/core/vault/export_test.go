/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package vault

import "time"

// SetNamespaceVersionTTL overrides how long v caches namespace versions.
func SetNamespaceVersionTTL(v *Vault, ttl time.Duration) { v.nsVersionTTL = ttl }
