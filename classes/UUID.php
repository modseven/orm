<?php
/**
 * The following class generates valid [RFC 4122][ref-rfc-4122] compliant
 * Universally Unique Identifiers (UUID) version 3, 4 and 5. UUIDs generated
 * validate using OSSP UUID Tool, and output for named-based UUIDs are exactly
 * the same. This is a pure PHP implementation.
 * Adapted from code published by [Andrew Moore][ref-php-94959].
 * [ref-rfc-4122]: http://www.ietf.org/rfc/rfc4122.txt
 * [ref-php-94959]: http://www.php.net/manual/en/function.uniqid.php#94959
 *
 * @author         Andrew Moore
 * @copyright  (c) 2007-2016  Kohana Team
 * @copyright  (c) since 2016 Koseven Team
 * @license        https://koseven.ga/LICENSE
 */

namespace Modseven\ORM;

class UUID
{
    /**
     * DNS namespace (RFC 4122)
     * @var string
     */
    public const DNS = '6ba7b810-9dad-11d1-80b4-00c04fd430c8';

    /**
     * URL namespace (RFC 4122)
     * @var string
     */
    public const URL = '6ba7b811-9dad-11d1-80b4-00c04fd430c8';

    /**
     * ISO object identifier namespace (RFC 4122)
     * @var string
     */
    public const OID = '6ba7b812-9dad-11d1-80b4-00c04fd430c8';

    /**
     * X.500 namespace (RFC 4122)
     * @var  string  X.500 namespace
     */
    public const X500 = '6ba7b814-9dad-11d1-80b4-00c04fd430c8';

    /**
     * NULL UUID string
     * @var string
     */
    public const NIL = '00000000-0000-0000-0000-000000000000';

    /**
     * Checks if a UUID has a valid format.
     *
     * @param string  UUID
     *
     * @return  bool
     */
    public static function valid(string $uuid): bool
    {
        return (preg_match('/^\{?[0-9a-f]{8}\-?[0-9a-f]{4}\-?[0-9a-f]{4}\-?[0-9a-f]{4}\-?[0-9a-f]{12}\}?$/i', $uuid) ===
                1);
    }

    /**
     * Convert a string UUID to binary format.
     *
     * @param string  uuid
     *
     * @return  string|bool
     *
     * @throws  Exception
     */
    public static function bin(string $uuid)
    {
        if ( ! self::valid($uuid))
        {
            throw new Exception('Could not convert to UUID to string. :uuid is no valid UUID', [':uuid' => $uuid]);
        }

        // Get hexadecimal components of uuid
        $hex = str_replace(['-', '{', '}'], '', $uuid);

        // Binary Value
        $bin = '';

        for ($i = 0, $max = strlen($hex); $i < $max; $i += 2)
        {
            // Convert each character to a bit
            $bin .= chr(hexdec($hex[$i] . $hex[$i + 1]));
        }

        return $bin;
    }

    /**
     * Convert a binary UUID to string format.
     *
     * @param string  uuid
     *
     * @return  string
     */
    public static function str(string $uuid): string
    {
        // String value
        $str = '';

        for ($i = 0, $max = strlen($uuid); $i < $max; $i++)
        {
            if ($i >= 4 && $i <= 10 && ($i % 2) === 0)
            {
                // Add dash at proper offsets
                $str .= '-';
            }

            // Convert each bit to an uppercase character
            $str .= sprintf('%02x', ord($uuid[$i]));
        }

        return $str;
    }

    /**
     * Version 3 UUIDs are named based. They require a namespace (another
     * valid UUID) and a value (the name). Given the same namespace and
     * name, the output is always the same.
     *
     * @param string  namespace
     * @param string  key name
     *
     * @return  string|bool
     *
     * @throws  Exception
     */
    public static function v3(string $namespace, string $name)
    {
        if ( ! self::valid($namespace))
        {
            // All namespaces must be valid UUIDs
            throw new Exception('Could not generate v3 UUID. :namespace is no valid UUID',
                [':namespace' => $namespace]
            );
        }

        // Get namespace in binary format
        $nstr = self::bin($namespace);

        // Calculate hash value
        $hash = md5($nstr . $name);

        return sprintf('%08s-%04s-%04x-%04x-%12s',
            // 32 bits for "time_low"
            substr($hash, 0, 8),

            // 16 bits for "time_mid"
            substr($hash, 8, 4),

            // 16 bits for "time_hi_and_version",
            // four most significant bits holds version number 3
            (hexdec(substr($hash, 12, 4)) & 0x0fff) | 0x3000,

            // 16 bits, 8 bits for "clk_seq_hi_res",
            // 8 bits for "clk_seq_low",
            // two most significant bits holds zero and one for variant DCE1.1
            (hexdec(substr($hash, 16, 4)) & 0x3fff) | 0x8000,

            // 48 bits for "node"
            substr($hash, 20, 12)
        );
    }

    /**
     * Version 4 UUIDs are pseudo-random.
     *
     * @return  string
     *
     * @throws    Exception
     */
    public static function v4(): string
    {
        try
        {
            $uuid = sprintf('%04x%04x-%04x-%04x-%04x-%04x%04x%04x',
                // 32 bits for "time_low"
                random_int(0, 0xffff), random_int(0, 0xffff),

                // 16 bits for "time_mid"
                random_int(0, 0xffff),

                // 16 bits for "time_hi_and_version",
                // four most significant bits holds version number 4
                random_int(0, 0x0fff) | 0x4000,

                // 16 bits, 8 bits for "clk_seq_hi_res",
                // 8 bits for "clk_seq_low",
                // two most significant bits holds zero and one for variant DCE1.1
                random_int(0, 0x3fff) | 0x8000,

                // 48 bits for "node"
                random_int(0, 0xffff), random_int(0, 0xffff), random_int(0, 0xffff)
            );
        }
        catch (\Exception $e)
        {
            throw new Exception($e->getMessage(), null, $e->getCode(), $e);
        }
        return $uuid;
    }

    /**
     * Version 5 UUIDs are named based. They require a namespace (another
     * valid UUID) and a value (the name). Given the same namespace and
     * name, the output is always the same.
     *
     * @param string   namespace
     * @param string   key name
     *
     * @return  string
     *
     * @throws  Exception
     */
    public static function v5(string $namespace, string $name)
    {
        if ( ! self::valid($namespace))
        {
            // All namespaces must be valid UUIDs
            throw new Exception('Could not generate v5 UUID. :namespace is no valid UUID',
                [':namespace' => $namespace]
            );
        }

        // Get namespace in binary format
        $nstr = self::bin($namespace);

        // Calculate hash value
        $hash = sha1($nstr . $name);

        return sprintf('%08s-%04s-%04x-%04x-%12s',
            // 32 bits for "time_low"
            substr($hash, 0, 8),

            // 16 bits for "time_mid"
            substr($hash, 8, 4),

            // 16 bits for "time_hi_and_version",
            // four most significant bits holds version number 5
            (hexdec(substr($hash, 12, 4)) & 0x0fff) | 0x5000,

            // 16 bits, 8 bits for "clk_seq_hi_res",
            // 8 bits for "clk_seq_low",
            // two most significant bits holds zero and one for variant DCE1.1
            (hexdec(substr($hash, 16, 4)) & 0x3fff) | 0x8000,

            // 48 bits for "node"
            substr($hash, 20, 12)
        );
    }
}
