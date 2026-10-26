/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.trino.jsonresultimpl;

import org.json.JSONObject;

/**
 * Supplies the next JSON result batch. JSONResultSet starts the fetch once the prefetch threshold of the current batch is consumed.
 *
 * @author vishva-15082
 */
public interface JSONResultBatchFetcher {

    JSONObject getNextBatch();

    default void closeStream() {
    }
}
