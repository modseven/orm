<?php
/**
 * Default auth user
 *
 * @copyright  (c) 2007-2016  Kohana Team
 * @copyright  (c) since 2016 Koseven Team
 * @license        https://koseven.ga/LICENSE
 */

namespace Modseven\ORM\Model\Auth;

use KO7\Valid;
use KO7\Validation;
use Modseven\ORM\ORM;
use Modseven\Database\DB;
use Modseven\ORM\Exception;

class User extends ORM
{
    /**
     * A user has many tokens and roles
     * @var array
     */
    protected array $_has_many = [
        'user_tokens' => ['model' => 'User_Token'],
        'roles'       => ['model' => 'Role', 'through' => 'roles_users'],
    ];

    /**
     * Rules for the user model. Because the password is _always_ a hash
     * when it's set,you need to run an additional not_empty rule in your controller
     * to make sure you didn't hash an empty string. The password rules
     * should be enforced outside the model or with a model helper method.
     *
     * @return array
     */
    public function rules() : array
    {
        return [
            'username' => [
                ['not_empty'],
                ['max_length', [':value', 32]],
                [[$this, 'unique'], ['username', ':value']],
            ],
            'password' => [
                ['not_empty'],
            ],
            'email'    => [
                ['not_empty'],
                ['email'],
                [[$this, 'unique'], ['email', ':value']],
            ],
        ];
    }

    /**
     * Filters to run when data is set in this model. The password filter
     * automatically hashes the password when it's set in the model.
     *
     * @return array
     */
    public function filters() : array
    {
        return [
            'password' => [
                [[\Modseven\Auth\Auth::instance(), 'hash']]
            ]
        ];
    }

    /**
     * Labels for fields in this model
     *
     * @return array Labels
     */
    public function labels() : array
    {
        return [
            'username' => 'username',
            'email'    => 'email address',
            'password' => 'password',
        ];
    }

    /**
     * Complete the login for a user by incrementing the logins and saving login timestamp
     *
     * @throws Exception
     * @throws \Modseven\ORM\Validation\Exception
     */
    public function complete_login() : void
    {
        if ($this->_loaded) {
            // Update the number of logins
            $this->logins = new Exception('logins + 1');

            // Set the last login date
            $this->last_login = time();

            // Save the user
            $this->update();
        }
    }

    /**
     * Tests if a unique key value exists in the database.
     *
     * @param mixed    the value to test
     * @param string   field name
     *
     * @return  boolean
     *
     * @throws Exception
     */
    public function unique_key_exists($value, ?string $field = null) : bool
    {
        if ($field === null)
        {
            // Automatically determine field by looking at the value
            $field = $this->unique_key($value);
        }

        try
        {
            $exists = (bool)DB::select([DB::expr('COUNT(*)'), 'total_count'])
                          ->from($this->_table_name)
                          ->where($field, '=', $value)
                          ->where($this->_primary_key, '!=', $this->pk())
                          ->execute($this->_db)
                          ->get('total_count');
        }
        catch (\KO7\Exception $e)
        {
            throw new Exception($e->getMessage(), null, $e->getCode(), $e);
        }

        return $exists;
    }

    /**
     * Allows a model use both email and username as unique identifiers for login
     *
     * @param string  unique value
     *
     * @return  string  field name
     */
    public function unique_key(string $value) : string
    {
        return Valid::email($value) ? 'email' : 'username';
    }

    /**
     * Password validation for plain passwords.
     *
     * @param array $values
     *
     * @return Validation
     */
    public static function get_password_validation($values)
    {
        return Validation::factory($values)
                         ->rule('password', 'min_length', [':value', 8])
                         ->rule('password_confirm', 'matches', [':validation', ':field', 'password']);
    }

    /**
     * Create a new user
     *
     * @param array $values
     * @param array $expected
     *
     * @return User|ORM
     *
     * @throws Exception
     * @throws \Modseven\ORM\Validation\Exception
     */
    public function create_user(array $values, array $expected)
    {
        // Validation for passwords
        $extra_validation = self::get_password_validation($values)
                                      ->rule('password', 'not_empty');

        return $this->values($values, $expected)->create($extra_validation);
    }

    /**
     * Update existing user
     *
     * @param      $values
     * @param null $expected
     *
     * @return User|ORM
     * @throws Exception
     * @throws \Modseven\ORM\Validation\Exception
     */
    public function update_user($values, $expected = null)
    {
        if (empty($values['password'])) {
            unset($values['password'], $values['password_confirm']);
        }

        // Validation for passwords
        $extra_validation = self::get_password_validation($values);

        return $this->values($values, $expected)->update($extra_validation);
    }
}