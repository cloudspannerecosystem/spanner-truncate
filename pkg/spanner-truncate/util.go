//
// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package truncate

import (
	"fmt"
)

// formatNumber formats the number with thousands separators.
// e.g. 12345 => "12,345"
func formatNumber(num uint64) string {
	if num < 1000 {
		return fmt.Sprintf("%d", num)
	}

	var parts []uint64
	for num > 0 {
		part := num % 1000
		parts = append(parts, part)
		num = num / 1000
	}

	var s string
	for i := 0; i < len(parts)-1; i++ {
		s = fmt.Sprintf(",%03d", parts[i]) + s
	}
	return fmt.Sprintf("%d", parts[len(parts)-1]) + s
}
