#
# Copyright (C) 2020 Grakn Labs
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#

java_binary(
    name = "backpressure-poc",
    main_class = "grakn.common.poc.BackpressurePOC",
    srcs = ["MockTransaction.java", "BackpressurePOC.java"],
    deps = ["//:common"],
)

java_binary(
    name = "cooperative-looping",
    main_class = "grakn.common.poc.CooperativeTest",
    srcs = ["MockTransaction.java", "CooperativeTest.java"],
    deps = ["//:common"],
)

java_binary(
    name = "noncooperative-looping",
    main_class = "grakn.common.poc.NonCooperativeTest",
    srcs = ["MockTransaction.java", "NonCooperativeTest.java"],
    deps = ["//:common"],
)