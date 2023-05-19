package api;

import java.time.LocalDateTime;
import java.util.Objects;

public record Price(String id,
                    LocalDateTime asOf,
                    PriceData payload) implements Comparable<Price> {
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Price price = (Price) o;
        return Objects.equals(id, price.id)
                && Objects.equals(asOf, price.asOf) &&
                Objects.equals(payload, price.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, asOf, payload);
    }

    @Override
    public int compareTo(Price other) {
        return asOf.compareTo(other.asOf);
    }
}