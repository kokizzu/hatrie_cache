package hatSql

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"strconv"
	"strings"
)

// SQLQueryFingerprint returns a privacy-safe SHA-256 fingerprint of a valid
// SQL query's structure. String and numeric literal values are replaced by
// their type, while identifiers, operators, and parameter positions remain
// part of the fingerprint.
func SQLQueryFingerprint(source string) (string, error) {
	if _, err := parseSQLQueryTemplate(source); err != nil {
		return "", err
	}
	tokens, err := Lex(source)
	if err != nil {
		return "", err
	}
	canonical := make([]byte, 0, len(source))
	for _, token := range tokens {
		if token.Kind() == TokenEOF {
			break
		}
		canonical = appendSQLFingerprintToken(canonical, token)
	}
	digest := sha256.Sum256(canonical)
	return hex.EncodeToString(digest[:]), nil
}

func appendSQLFingerprintToken(destination []byte, token Token) []byte {
	value := token.Text()
	switch token.Kind() {
	case TokenString:
		value = "string"
	case TokenNumber:
		value = "real"
		if !strings.ContainsAny(token.Text(), ".eE") {
			value = "integer"
		}
	case TokenParameter:
		if index, err := strconv.Atoi(token.Text()); err == nil && index > 0 {
			value = strconv.Itoa(index)
		}
	default:
		value = formatSQLToken(token)
	}

	destination = append(destination, byte(token.Kind()))
	var length [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(length[:], uint64(len(value)))
	destination = append(destination, length[:n]...)
	destination = append(destination, value...)
	return destination
}
