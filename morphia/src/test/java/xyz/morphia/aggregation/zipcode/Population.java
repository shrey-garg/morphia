/*
 * Copyright (c) 2008-2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package xyz.morphia.aggregation.zipcode;

import xyz.morphia.annotations.AlsoLoad;
import xyz.morphia.annotations.Entity;
import xyz.morphia.annotations.Id;
import xyz.morphia.annotations.Property;

@Entity
public class Population {
    @Id
    private String state;
    @Property("totalPop")
    @AlsoLoad("avgCityPop")
    private Long population;

    public String getState() {
        return state;
    }

    public Long getPopulation() {
        return population;
    }

    @Override
    public String toString() {
        return String.format("Population{population=%d, state='%s'}", population, state);
    }
}
