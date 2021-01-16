// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"
	"fmt"

	"github.com/pingcap/tidb/util/chunk"
)

// make sure `TableReaderExecutor` implements `Executor`.
var _ Executor = &MaterializedViewReaderExecutor{}

type MaterializedViewReaderExecutor struct {
	baseExecutor
}

func (mvr *MaterializedViewReaderExecutor) Open(ctx context.Context) error {
	fmt.Println("MVR Open")
	return nil
}

func (mvr *MaterializedViewReaderExecutor) Next(ctx context.Context, req *chunk.Chunk) error {
	fmt.Println("MVR Next")
	return nil
}

func (mvr *MaterializedViewReaderExecutor) Close() error {
	fmt.Println("MVR Close")
	return nil
}
