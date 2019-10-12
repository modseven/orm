<?php
/**
 * Bcrypt Auth driver.
 *
 * @copyright  (c) 2007-2016  Kohana Team
 * @copyright  (c) since 2016 Koseven Team
 * @license        http:/koseven.ga/license
 */

namespace Modseven\ORM\Auth;

use KO7\Cookie;

use Ko7\Request;
use Modseven\Auth\Auth;
use Modseven\ORM\Exception;
use Modseven\ORM\Model\User;
use Modseven\ORM\Model\Role;
use Modseven\ORM\Model\User\Token;

class Bcrypt extends Auth
{

    /**
     * Bcrypt Auth constructor.
     *
     * @param array $config Configuration Values
     *
     * @throws Exception
     */
    public function __construct(array $config = [])
    {
        if ( ! isset($config['cost']) || ! is_numeric($config['cost']) || $config['cost'] < 10)
        {
            throw new Exception(__CLASS__ . ' cost parameter must be set and must be integer >= 10');
        }
        parent::__construct($config);
    }

    /**
     * Logs a user in, based on the authautologin cookie.
     *
     * @return  mixed
     */
    public function auto_login()
    {
        if ($token = Cookie::get('authautologin'))
        {
            // Load the token and user
            $token = \Modseven\ORM\ORM::factory(Token::class, ['token' => $token]);

            if ($token->loaded() && $token->user->loaded())
            {
                if (hash_equals($token->user_agent, hash('sha256', Request::$user_agent)))
                {
                    // Save the token to create a new unique token
                    $token->save();

                    // Set the new token
                    Cookie::set('authautologin', $token->token, $token->expires - time());

                    // Complete the login with the found data
                    $this->complete_login($token->user);

                    // Automatic login was successful
                    return $token->user;
                }

                // Token is invalid
                $token->delete();
            }
        }

        return false;
    }

    /**
     * Gets the currently logged in user from the session (with auto_login check).
     * Returns $default if no user is currently logged in.
     *
     * @param mixed $default to return in case user isn't logged in
     *
     * @return  mixed
     */
    public function get_user($default = null)
    {
        $user = parent::get_user($default);

        // check for "remembered" login
        if (($user === $default) && ($user = $this->auto_login()) === false)
        {
            return $default;
        }

        return $user;
    }

    /**
     * Log a user out and remove any autologin cookies.
     *
     * @param boolean $destroy    completely destroy the session
     * @param boolean $logout_all remove all tokens for user
     *
     * @return  boolean
     */
    public function logout(bool $destroy = false, bool $logout_all = false) : bool
    {
        // Set by force_login()
        $this->_session->delete('auth_forced');

        if ($token = Cookie::get('authautologin'))
        {
            // Delete the autologin cookie to prevent re-login
            Cookie::delete('authautologin');

            // Clear the autologin token from the database
            $token = \Modseven\ORM\ORM::factory(Token::class, ['token' => $token]);

            if ($logout_all && $token->loaded())
            {
                // Delete all user tokens. This isn't the most elegant solution but does the job
                $tokens = \Modseven\ORM\ORM::factory(Token::class)->where('user_id', '=', $token->user_id)->find_all();

                foreach ($tokens as $_token)
                {
                    $_token->delete();
                }
            }
            elseif ($token->loaded())
            {
                $token->delete();
            }
        }

        return parent::logout($destroy);
    }

    /**
     * Performs login on user
     *
     * @param string $username Username
     * @param string $password Password
     * @param bool   $remember Remembers login
     *
     * @return boolean
     */
    protected function _login(string $username, string $password, bool $remember = false) : bool
    {
        // Load the user
        $user = \Modseven\ORM\ORM::factory(User::class);
        $user->where($user->unique_key($username), '=', $username)->find();

        if ($user->loaded() && password_verify($password, $user->password) && $user->has('roles', \Modseven\ORM\ORM::factory(Role::class, ['name' => 'login'])))
        {
            if ($remember === true)
            {
                // Token data
                $data = [
                    'user_id'    => $user->pk(),
                    'expires'    => time() + $this->_config['lifetime'],
                    'user_agent' => hash('sha256', Request::$user_agent),
                ];

                // Create a new autologin token
                $token = \Modseven\ORM\ORM::factory(Token::class)
                            ->values($data)
                            ->create();

                // Set the autologin cookie
                Cookie::set('authautologin', $token->token, $this->_config['lifetime']);
            }

            $this->complete_login($user);
            $user->complete_login();

            if (password_needs_rehash($user->password, PASSWORD_BCRYPT, ['cost' => $this->_config['cost']])) {
                $user->password = $password;
                $user->save();
            }

            return true;
        }

        return false;
    }

    /**
     * Compare password with original (hashed). Works for current (logged in) user
     *
     * @param string $password
     *
     * @return  bool
     */
    public function check_password(string $password) : bool
    {
        $user = $this->get_user();

        if ( ! $user)
        {
            return false;
        }

        return password_verify($password, $user->password);
    }

    /**
     * Get the stored password for a username.
     *
     * @param mixed $user username string, or user ORM object
     *
     * @return  string
     */
    public function password($user) : string
    {
        if ( ! is_object($user))
        {
            $username = $user;

            // Load the user
            $user = \Modseven\ORM\ORM::factory(User::class);
            $user->where($user->unique_key($username), '=', $username)->find();
        }

        return $user->password;
    }

    /**
     * Forces a user to be logged in, without specifying a password.
     *
     * @param mixed   $user                   username string, or user ORM object
     * @param boolean $mark_session_as_forced mark the session as forced
     */
    public function force_login($user, bool $mark_session_as_forced = false) : void
    {
        if ( ! is_object($user))
        {
            $username = $user;

            // Load the user
            $user = \Modseven\ORM\ORM::factory(User::class);
            $user->where($user->unique_key($username), '=', $username)->find();
        }

        if ($mark_session_as_forced === true)
        {
            // Mark the session as forced, to prevent users from changing account information
            $this->_session->set('auth_forced', true);
        }

        // Run the standard completion
        $this->complete_login($user);
    }

    /**
     * @inheritdoc
     */
    public function hash(string $str) : string
    {
        return password_hash($str, PASSWORD_BCRYPT, [
            'cost' => $this->_config['cost']
        ]);
    }

}
